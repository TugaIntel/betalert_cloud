from django.shortcuts import render
from .models import Match
from django.utils import timezone
from datetime import datetime


def home(request):
    current_date = timezone.localdate()
    start_of_day = datetime.combine(current_date, datetime.min.time(), tzinfo=timezone.get_current_timezone())
    end_of_day = datetime.combine(current_date, datetime.max.time(), tzinfo=timezone.get_current_timezone())

    # Fetch the top 25 matches based on user_count where the match is today
    matches = Match.objects.filter(
        match_time__range=(start_of_day, end_of_day)
    ).order_by('-user_count')[:50]

    return render(request, 'matches/home.html', {'matches': matches})
