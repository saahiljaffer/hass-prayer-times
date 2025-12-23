"""Coordinator for the Islamic prayer times integration."""

from __future__ import annotations

from datetime import date, datetime, timedelta
import logging
from typing import Any
import os

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_LATITUDE, CONF_LONGITUDE
from homeassistant.core import CALLBACK_TYPE, HomeAssistant, callback
from homeassistant.helpers.event import async_track_point_in_time
from homeassistant.util import dt as dt_util
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed
import sqlite3

from .const import DOMAIN

_LOGGER = logging.getLogger(__name__)

type IslamicPrayerTimesConfigEntry = ConfigEntry[IslamicPrayerDataUpdateCoordinator]


class IslamicPrayerDataUpdateCoordinator(DataUpdateCoordinator[dict[str, datetime]]):
    """Islamic Prayer Client Object."""

    config_entry: IslamicPrayerTimesConfigEntry

    def __init__(
        self, hass: HomeAssistant, config_entry: IslamicPrayerTimesConfigEntry
    ) -> None:
        """Initialize the Islamic Prayer client."""
        super().__init__(
            hass,
            _LOGGER,
            config_entry=config_entry,
            name=DOMAIN,
        )
        self.latitude = config_entry.data[CONF_LATITUDE]
        self.longitude = config_entry.data[CONF_LONGITUDE]
        self.event_unsub: CALLBACK_TYPE | None = None
        self.db_path = os.path.join(os.path.dirname(__file__), "prayertimes.db")

    def _fetch_prayer_times_from_db(self, for_date: date) -> dict[str, Any]:
        """Fetch prayer times from database (runs in executor)."""
        _LOGGER.debug("fetching prayer times")
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            query = """
            SELECT fajr, sunrise, dhuhr, sunset, maghrib, midnight
            FROM times
            WHERE month = ? AND day = ?
            """
        
            cursor.execute(query, (for_date.month, for_date.day))
            row = cursor.fetchone()
            conn.close()
            
            if not row:
                raise UpdateFailed(
                    f"No prayer times found in database for date: {for_date}"
                )

            # Convert time strings to ISO8601 datetimes
            def time_to_iso8601(time_str: str, for_date: date) -> str:
                """Convert time string like '05:31' to ISO8601 datetime string."""
                hour, minute = time_str.split(":")
                local_dt = datetime.combine(
                    for_date, 
                    datetime.strptime(f"{hour}:{minute}", "%H:%M").time()
                )
                # Make it timezone-aware using local timezone
                aware_dt = dt_util.as_local(local_dt)

                # Adjust for DST if active
                if aware_dt.dst():
                    aware_dt = aware_dt + timedelta(hours=1)

                return aware_dt.isoformat()
            

            # Return in the same format as PrayerTimesCalculator
            return {
                "Fajr": time_to_iso8601(row[0], for_date),
                "Sunrise": time_to_iso8601(row[1], for_date),
                "Dhuhr": time_to_iso8601(row[2], for_date),
                "Sunset": time_to_iso8601(row[3], for_date),
                "Maghrib": time_to_iso8601(row[4], for_date),
                "Midnight": time_to_iso8601(row[5], for_date),
            }
        except Exception as err:
            _LOGGER.error("Error fetching prayer times from database: %s", err)
            raise UpdateFailed(f"Failed to fetch prayer times: {err}") from err

    async def get_new_prayer_times(self, for_date: date) -> dict[str, Any]:
        """Fetch prayer times for the specified date from SQLite database."""
        return await self.hass.async_add_executor_job(
            self._fetch_prayer_times_from_db, for_date
        )

    @callback
    def async_schedule_future_update(self, midnight_dt: datetime) -> None:
        """Schedule future update for sensors.

        The least surprising behaviour is to load the next day's prayer times only
        after the current day's prayers are complete. We will take the fiqhi opinion
        that Isha should be prayed before Islamic midnight (which may be before or after 12:00 midnight),
        and thus we will switch to the next day's timings at Islamic midnight.

        The +1s is to ensure that any automations predicated on the arrival of Islamic midnight will run.

        """
        _LOGGER.debug(f"Scheduling next update for Islamic prayer times for ${midnight_dt}")

        # now = dt_util.now()
        # next_midnight = (now + timedelta(days=1)).replace(
        #     hour=0, minute=0, second=0, microsecond=0
        # )
        
        # self.event_unsub = async_track_point_in_time(
        #     self.hass, self.async_request_update, next_midnight
        # )

        self.event_unsub = async_track_point_in_time(
            self.hass, self.async_request_update, midnight_dt + timedelta(seconds=1)
        )

    async def async_request_update(self, _: datetime) -> None:
        """Request update from coordinator."""
        await self.async_request_refresh()

    async def _async_update_data(self) -> dict[str, datetime]:
        """Update sensors with new prayer times.

        Prayer time calculations "roll over" at 12:00 midnight - but this does not mean that all prayers
        occur within that Gregorian calendar day. For instance Jasper, Alta. sees Isha occur after 00:00 in the summer.
        It is similarly possible (albeit less likely) that Fajr occurs before 00:00.

        As such, to ensure that no prayer times are "unreachable" (e.g. we always see the Isha timestamp pass before loading the next day's times),
        we calculate 3 days' worth of times (-1, 0, +1 days) and select the appropriate set based on Islamic midnight.

        The calculation is inexpensive, so there is no need to cache it.
        """

        # Zero out the us component to maintain consistent rollover at T+1s
        now = dt_util.now().replace(microsecond=0)
        yesterday_times = await self.get_new_prayer_times((now - timedelta(days=1)).date())
        today_times = await self.get_new_prayer_times(now.date())
        tomorrow_times = await self.get_new_prayer_times((now + timedelta(days=1)).date())

        if (
            yesterday_midnight := dt_util.parse_datetime(yesterday_times["Midnight"])
        ) and now <= yesterday_midnight:
            prayer_times = yesterday_times
        elif (
            tomorrow_midnight := dt_util.parse_datetime(today_times["Midnight"])
        ) and now > tomorrow_midnight:
            prayer_times = tomorrow_times
        else:
            prayer_times = today_times

        # prayer_times = today_times

        # # introduced in prayer-times-calculator 0.0.8
        # prayer_times.pop("date", None)

        prayer_times_info: dict[str, datetime] = {}
        for prayer, time in prayer_times.items():
            if prayer_time := dt_util.parse_datetime(time):
                prayer_times_info[prayer] = dt_util.as_utc(prayer_time)

        self.async_schedule_future_update(prayer_times_info["Midnight"])
        return prayer_times_info