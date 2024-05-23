from django.urls import path
from . import views

urlpatterns = [
    path('submit_query/', views.json_form, name='submit_query'),
    path('success/<str:job_id>/', views.load_job_data, name='job-success'),
]
