from django.urls import path
from .views import home

urlpatterns = [
    path('', home, name='home'),  # Default to today
    path('matches/<str:day>/', home, name='matches_by_day'),
]
