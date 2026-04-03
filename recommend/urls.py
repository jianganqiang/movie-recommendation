from django.urls import path
from . import views

urlpatterns = [
    path('', views.index, name='index'),
    path('accounts/register/', views.register_view, name='register'),
    path('accounts/login/', views.login_view, name='login'),
    path('accounts/logout/', views.logout_view, name='logout'),
    path('accounts/profile/', views.profile_view, name='profile'),
    path('movies/', views.movie_list, name='movie_list'),
    path('movies/<int:pk>/', views.movie_detail, name='movie_detail'),
    path('movies/<int:pk>/rate/', views.movie_detail, name='rate_movie'),  # 实际由POST处理
    path('movies/<int:pk>/delete-rating/', views.delete_rating, name='delete_rating'),
]