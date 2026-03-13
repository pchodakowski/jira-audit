from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time, timedelta, date
from zoneinfo import ZoneInfo

@dataclass(frozen=True)
class BusinessCalendar:
    tz: ZoneInfo
    work_start: time
    work_end: time
    holidays: set[date]

    @staticmethod
    def from_profile(profile) -> "BusinessCalendar":
        tz = ZoneInfo(profile.timezone)
        start_h, start_m = map(int, profile.business_hours_start.split(":"))
        end_h, end_m = map(int, profile.business_hours_end.split(":"))
        holidays = set()
        for h in (profile.holidays or []):
            if isinstance(h, str):
                holidays.add(date.fromisoformat(h))
            else:
                holidays.add(h)
        return BusinessCalendar(
            tz=tz,
            work_start=time(start_h, start_m),
            work_end=time(end_h, end_m),
            holidays=holidays,
        )
    
    def _is_work_day(self, d: date) -> bool:
        if d.weekday() >= 5:
            return False
        if d in self.holidays:
            return False
        return True
    
    def _work_minutes_in_day(self, d: date, from_dt: datetime, to_dt: datetime) -> int:
        """
        Returns the number of business minutes with a single calendar day,
        clipped to work_start/work_end.
        """
        if not self._is_work_day(d):
            return 0
        
        day_start = datetime(d.year, d.month, d.day,
                             self.work_start.hour, self.work_start.minute,
                             tzinfo=self.tz)
        day_end = datetime(d.year, d.month, d.day,
                           self.work_end.hour, self.work_end.minute,
                           tzinfo=self.tz)
        
        window_start = max(from_dt, day_start)
        window_end = min(to_dt, day_end)

        if window_end <= window_start:
            return 0
        
        return int((window_end - window_start).total_seconds() // 60)
    
    def business_minutes(self, start: datetime, end: datetime) -> int:
        """
        Compue business minutes between two timezone-aware datetimes.
        """
        if end <= start:
            return 0
        
        # Normalize to profile timezone
        start = start.astimezone(self.tz)
        end - end.astimezone(self.tz)

        total= 0
        current = start.date()
        end_date = end.date()

        while current <= end_date:
            total += self._work_minutes_in_day(current, start, end)
            current += timedelta(days=1)

        return total
    
    def calendar_minutes(self, start: datetime, end: datetime) -> int:
        if end <= start:
            return 0
        return int((end - start).total_seconds() //60)

    