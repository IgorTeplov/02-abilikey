# Generated by Django 5.1.2 on 2025-05-26 18:04

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('cache', '0001_initial'),
    ]

    operations = [
        migrations.AddField(
            model_name='reel',
            name='account_username',
            field=models.CharField(blank=True, max_length=64),
        ),
        migrations.AddField(
            model_name='reel',
            name='code',
            field=models.CharField(blank=True, max_length=32),
        ),
        migrations.AddField(
            model_name='reel',
            name='model',
            field=models.CharField(blank=True, max_length=32),
        ),
        migrations.AddField(
            model_name='reel',
            name='sound_rapid_id',
            field=models.CharField(blank=True, max_length=32),
        ),
        migrations.AddField(
            model_name='reel',
            name='sound_type',
            field=models.CharField(blank=True, max_length=32),
        ),
        migrations.AddField(
            model_name='reel',
            name='video_duration',
            field=models.FloatField(blank=True, default=0),
        ),
        migrations.AddField(
            model_name='reel',
            name='video_hash',
            field=models.CharField(blank=True, db_index=True, max_length=32),
        ),
    ]
