from django.contrib import admin
from .models import Movie, Rating

@admin.register(Movie)
class MovieAdmin(admin.ModelAdmin):
    list_display = ('id', 'title', 'genres', 'avg_rating', 'rating_count')
    search_fields = ('title',)
    list_filter = ('genres',)

@admin.register(Rating)
class RatingAdmin(admin.ModelAdmin):
    list_display = ('user', 'movie', 'rating', 'timestamp')
    list_filter = ('rating',)