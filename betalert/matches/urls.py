# matches/urls.py

from django.urls import path
from .views import upcoming_matches

urlpatterns = [
    path('', upcoming_matches, name='upcoming_matches'),
]
