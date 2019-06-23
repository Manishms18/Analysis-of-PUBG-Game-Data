CREATE EXTERNAL TABLE pubg_new(
         Id String,
         groupId String,
         matchId String,
         assists String,
         boosts int,
         damageDealt float,
        DBNOs int,
        headshotKills int,
        heals int,
        killPlace int,
        killPoints int,
        kills int,
        killStreaks int,
        longestKill float,
        maxPlace int,
        numGroups int,
        revives int,
        rideDistance float,
        roadKills int,
        swimDistance float,
        teamKills int,
        vehicleDestroys int,
        walkDistance float,
        weaponsAcquired int,
        winPoints int,
        winPlacePerc float)
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
LOCATION '/loudacre/PUBG/'
TBLPROPERTIES ("skip.header.line.count"="1");
