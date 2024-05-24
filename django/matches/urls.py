from django.urls import path
from .views import home

urlpatterns = [
    path('', home, name='home'),
    path('matches/<str:day>/', home, name='matches_by_day'),
]
