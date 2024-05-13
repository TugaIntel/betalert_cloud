from django.shortcuts import render
from datetime import datetime, timedelta
from django.utils import timezone
from .models import Match


def home(request, day='today'):
    current_date = timezone.now()
    if day == 'yesterday':
        date_to_show = current_date - timedelta(days=1)
    elif day == 'tomorrow':
        date_to_show = current_date + timedelta(days=1)
    else:
        date_to_show = current_date

    start_of_day = datetime.combine(date_to_show.date(), datetime.min.time(), tzinfo=timezone.get_current_timezone())
    end_of_day = datetime.combine(date_to_show.date(), datetime.max.time(), tzinfo=timezone.get_current_timezone())

    matches = Match.objects.filter(
        match_time__range=(start_of_day, end_of_day)
    ).order_by('-user_count')[:50]
    return render(request, 'matches/home.html', {'matches': matches, 'current_day': day})
