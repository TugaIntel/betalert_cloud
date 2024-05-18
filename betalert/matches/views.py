from django.shortcuts import render
from datetime import datetime, timedelta
from django.utils import timezone
from .models import Match
import logging

logger = logging.getLogger(__name__)


def home(request, day='today'):
    try:
        current_date = timezone.now()
        if day == 'yesterday':
            date_to_show = current_date - timedelta(days=1)
        elif day == 'tomorrow':
            date_to_show = current_date + timedelta(days=1)
        else:
            date_to_show = current_date

        start_of_day = timezone.make_aware(datetime.combine(date_to_show.date(), datetime.min.time()))
        end_of_day = timezone.make_aware(datetime.combine(date_to_show.date(), datetime.max.time()))

        matches = Match.objects.filter(
            match_time__range=(start_of_day, end_of_day)
        ).order_by('-user_count')[:50]

        logger.debug(f"Fetched {len(matches)} matches for {day}")

        return render(request, 'matches/home.html', {'matches': matches, 'current_day': day})

    except Exception as e:
        logger.error(f"Error fetching matches for {day}: {str(e)}")
        return render(request, 'matches/home.html', {'error': 'Error fetching matches'})
