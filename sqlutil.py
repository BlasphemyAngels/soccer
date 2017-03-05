# !/usr/bin/python
# _*_coding: utf-8_*_

"""
    Create 2017-2-25
    Author: ccl
"""

import os
import sqlite3
import zipfile
import datetime
import pandas as pd
import numpy as np


class DB(object):
    """handle the database"""
    def __init__(self, analysis_player_num):
        self.conn = None
        self.database = "data/{}".format(self._unpack_data())
        self.analysis_player_num = analysis_player_num

    # pylint: disable=R0201
    def _unpack_data(self):
        """
            unpack the source data
        """
        filename = "data/soccer.zip"
        zpfile = zipfile.ZipFile(filename, 'r')
        fn = zpfile.namelist()[0]
        data = zpfile.read(fn)
        self.datapath = 'data/{}'.format(fn)
        with open('data/'+fn, 'wb') as f:
            f.write(data)
        return fn

    def delete_data(self):
        """
            delete the data of the source
        """
        if os.path.exists(self.datapath):
            os.remove(self.datapath)

    def connect(self):
        """
            connect to the database and return the connection
        @return: the connection
        """
        self.conn = sqlite3.connect(self.database)
        self.conn.row_factory = sqlite3.Row

    # pylint: disable=too-many-locals
    def get_player_info(self):
        """
            get the player info from the database
        @return: the dataframe of the player info
        """
        self.connect()
        sql = "select * from Player limit {}".format(self.analysis_player_num)
        cursor = self.conn.cursor()
        cursor.execute(sql)
        players = cursor.fetchall()
        player_name_list = []
        player_birth_list = []
        player_height_list = []
        player_weight_list = []
        player_rating_list = []
        player_team_list = []
        player_country_list = []
        player_team_num_list = []
        for player in players:
            player_name_list.append(player['Player_name'])
            player_birth_list.append(player['birthday'])
            player_height_list.append(player['height'])
            player_weight_list.append(player['weight'])
            player_id = player['player_api_id']
            player_rating = self._get_the_mean_rating(player_id, cursor)
            player_rating_list.append(player_rating)
            last_team, last_country, team_nums =\
                self._get_teams_and_countries(cursor, player_id, player_rating)
            player_team_list.append(last_team)
            player_country_list.append(last_country)
            player_team_num_list.append(team_nums)
        player_name_se = pd.Series(player_name_list, name='name')
        player_birth_se = pd.Series(player_birth_list, name='birthday')
        player_height_se = pd.Series(player_height_list, name='height')
        player_weight_se = pd.Series(player_weight_list, name='weight')
        player_age_list =\
            [self._get_player_age(birth) for birth in player_birth_list]
        player_age_se = pd.Series(player_age_list, name='age')
        player_rating_se = pd.Series(player_rating_list, name='mean_rating')
        player_team_se = pd.Series(player_team_list, name='team')
        player_country_se = pd.Series(player_country_list, name='country')
        player_team_nums_se = pd.Series(player_team_num_list, name='team_nums')
        player_df = pd.concat([player_name_se, player_birth_se, player_age_se,\
                    player_height_se, player_weight_se, player_rating_se,\
                    player_team_se, player_country_se, player_team_nums_se],\
                    axis=1)
        print(player_df.head())
        self.close()
        return player_df

    # pylint: disable=R0201
    def _get_teams_and_countries(self, cursor, player_id, rating):
        if rating > 1:
            all_football_nums = reversed(range(1, 12))
            for num in all_football_nums:
                all_team_id = cursor.execute(
                    "SELECT home_team_api_id, country_id FROM Match WHERE\
                    home_player_{} = '{}'".format(num, player_id)).fetchall()
                if len(all_team_id) > 0:
                    number_unique_teams =\
                        len(np.unique(np.array(all_team_id)[:, 0]))
                    last_team_id = all_team_id[-1]['home_team_api_id']
                    last_country_id = all_team_id[-1]['country_id']
                    last_country =\
                        cursor.execute("SELECT name FROM Country WHERE id = \
                                '{}'".format(last_country_id)).fetchall()[0][0]
                    last_team = cursor.execute(
                        "SELECT team_long_name FROM Team WHERE team_api_id = \
                        '{}'".format(last_team_id)).fetchall()[0][0]
                    return last_team, last_country, number_unique_teams
        return None, None, 0

    # pylint: disable=R0201
    def _get_the_mean_rating(self, player_id, cursor):
        """
            get the mean rating of a player
        @parm player_id: the player's id
        @return: the player's mean rating
        """
        sql = "select overall_rating from Player_attributes where player_api_id\
            = {}".format(player_id)
        cursor.execute(sql)
        ratings = cursor.fetchall()
        ratings = np.array(ratings, dtype=np.float)[:, 0]
        mean_rating = np.nanmean(ratings)
        return mean_rating

    # pylint: disable=R0201
    def _get_player_age(self, birth):
        """
            transform the birthday to the age
        @param birth: the birthday
        @return: the age
        """
        birth = birth.split(' ')[0]
        now = datetime.datetime.strptime('2017-2-22', '%Y-%m-%d').date()
        born = datetime.datetime.strptime(birth, '%Y-%m-%d').date()
        return now.year - born.year - \
            ((now.month, now.day) < (born.month, born.day))

    def close(self):
        """close the connection"""
        if self.conn:
            self.conn.close()
        self.delete_data()
