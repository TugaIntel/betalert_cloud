from django.shortcuts import render
from .models import Match
from django.utils import timezone
from datetime import timedelta


def upcoming_matches(request):
    # Get the current time with timezone support
    current_time = timezone.now()

    # Calculate the time for 'current time minus 3 hours'
    time_threshold = current_time - timedelta(hours=3)

    # Filter matches that are either in progress or not started and match_time is greater than 'now minus 3 hours'
    matches = Match.objects.filter(
        match_time__gt=time_threshold,
        match_status__in=['in progress', 'notstarted'],
        reputation_tier__in=['top', 'medium']
    ).order_by('match_time')  # Ordering by match time to display them chronologically

    return render(request, 'matches/upcoming_matches.html', {'matches': matches})
