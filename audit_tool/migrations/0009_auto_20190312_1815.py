# -*- coding: utf-8 -*-
# Generated by Django 1.11.13 on 2019-03-12 18:15
from __future__ import unicode_literals

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('audit_tool', '0008_remove_comment_is_top_level'),
    ]

    operations = [
        migrations.CreateModel(
            name='CommentVideo',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('video_id', models.CharField(unique=True, max_length=15)),
            ],
        ),
        migrations.AddField(
            model_name='comment',
            name='comment_id',
            field=models.CharField(db_index=True, default='', max_length=50),
        ),
        migrations.RemoveField(
            model_name='comment',
            name='parent_id',
        ),
        migrations.AddField(
            model_name='comment',
            name='parent',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.CASCADE, to='audit_tool.Comment'),
        ),
        migrations.AlterField(
            model_name='comment',
            name='id',
            field=models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID'),
        ),
        migrations.AlterField(
            model_name='comment',
            name='like_count',
            field=models.IntegerField(default=0),
        ),
        migrations.AlterField(
            model_name='comment',
            name='reply_count',
            field=models.IntegerField(default=0),
        ),
        migrations.AlterField(
            model_name='comment',
            name='user',
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='user_comments', to='audit_tool.YoutubeUser'),
        ),
        migrations.AlterField(
            model_name='youtubeuser',
            name='channel_id',
            field=models.CharField(db_index=True, max_length=50),
        ),
        migrations.AlterField(
            model_name='youtubeuser',
            name='name',
            field=models.CharField(max_length=30),
        ),
        migrations.AlterUniqueTogether(
            name='comment',
            unique_together=set([]),
        ),
        migrations.RemoveField(
            model_name='comment',
            name='time_stamp',
        ),
        migrations.RemoveField(
            model_name='comment',
            name='video_id',
        ),
        migrations.AddField(
            model_name='comment',
            name='video',
            field=models.ForeignKey(default=None, on_delete=django.db.models.deletion.CASCADE, related_name='video_comments', to='audit_tool.CommentVideo'),
            preserve_default=False,
        ),
    ]
