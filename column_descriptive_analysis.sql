set hive.cli.print.header=true;
select avg(boosts) as Average_boosts, min(boosts) as min_boosts, max(boosts) as Max_boosts, 
variance(boosts) as variance, stddev_pop(boosts) as Standard_Deviation, 
corr(boosts,winplaceperc) as Correlation from pubg_new ;

set hive.cli.print.header=true;
select avg(damagedealt) as Average_DD, min(damagedealt) as min_DD, max(damagedealt) as Max_DD, 
variance(damagedealt) as variance, stddev_pop(damagedealt) as Standard_Deviation, 
corr(damagedealt,winplaceperc) as Correlation from pubg_new ;

set hive.cli.print.header=true;
select avg(DBNOs) as Average_DBNOs, min(DBNOs) as min_DBNOs, max(DBNOs) as Max_DBNOs, 
variance(DBNOs) as variance, stddev_pop(DBNOs) as Standard_Deviation, 
corr(DBNOs,winplaceperc) as Correlation from pubg_new ;

set hive.cli.print.header=true;
select avg(headshotkills) as Average_HSK, min(headshotkills) as min_HSK, max(headshotkills) as Max_HSK, 
variance(headshotkills) as variance, stddev_pop(headshotkills) as Standard_Deviation, 
corr(headshotkills,winplaceperc) as Correlation from pubg_new ;

set hive.cli.print.header=true;
select avg(winplaceperc) as Average_WPP, min(winplaceperc) as min_WPP, max(winplaceperc) as Max_WPP,
variance(winplaceperc) as variance, stddev_pop(winplaceperc) as Standard_Deviation from pubg_new ;

set hive.cli.print.header=true;
select avg(heals) as Average_heals, min(heals) as min_heals, max(heals) as Max_heals, 
variance(heals) as variance, stddev_pop(heals) as Standard_Deviation, 
corr(heals,winplaceperc) as Correlation from pubg_new ;

set hive.cli.print.header=true;
select avg(killPlace) as Average_KP, min(killplace) as min_kp, max(killplace) as Max_kp, 
variance(killplace) as variance, stddev_pop(killplace) as Standard_Deviation, 
corr(killplace,winplaceperc) as Correlation from pubg_new ;

set hive.cli.print.header=true;
select avg(longestkill) as Average_LK, min(longestkill) as min_LK, max(longestkill) as Max_LK, 
variance(longestkill) as variance, stddev_pop(longestkill) as Standard_Deviation, 
corr(longestkill,winplaceperc) as Correlation from pubg_new ;

set hive.cli.print.header=true;
select avg(kills) as Average_kills, min(kills) as min_kills, max(kills) as Max_kills, 
variance(kills) as variance, stddev_pop(kills) as Standard_Deviation, 
corr(kills,winplaceperc) as Correlation from pubg_new ;

set hive.cli.print.header=true;
select avg(revives) as Average_revives, min(revives) as min_revives, max(revives) as Max_revives, 
variance(revives) as variance, stddev_pop(revives) as Standard_Deviation, 
corr(revives,winplaceperc) as Correlation from pubg_new ;

set hive.cli.print.header=true;
select avg(ridedistance) as Average_RD, min(ridedistance) as min_RD, max(ridedistance) as Max_RD, 
variance(ridedistance) as variance, stddev_pop(ridedistance) as Standard_Deviation, 
corr(ridedistance,winplaceperc) as Correlation from pubg_new ;

set hive.cli.print.header=true;
select avg(swimdistance) as Average_SD, min(swimdistance) as min_SD, max(swimdistance) as Max_swimdistance, 
variance(swimdistance) as variance, stddev_pop(swimdistance) as Standard_Deviation, 
corr(swimdistance,winplaceperc) as Correlation from pubg_new ;

set hive.cli.print.header=true;
select avg(walkdistance) as Average_WD, min(walkdistance) as min_WD, max(walkdistance) as Max_WD, 
variance(walkdistance) as variance, stddev_pop(walkdistance) as Standard_Deviation, 
corr(walkdistance,winplaceperc) as Correlation from pubg_new ;

set hive.cli.print.header=true;
select avg(teamkills) as Average_TK, min(teamkills) as min_TK, max(teamkills) as Max_TK, 
variance(teamkills) as variance, stddev_pop(teamkills) as Standard_Deviation, 
corr(teamkills,winplaceperc) as Correlation from pubg_new ;

set hive.cli.print.header=true;
select avg(weaponsacquired) as Average_WA, min(weaponsacquired) as min_WA, max(weaponsacquired) as Max_WA, 
variance(weaponsacquired) as variance, stddev_pop(weaponsacquired) as Standard_Deviation, 
corr(weaponsacquired,winplaceperc) as Correlation from pubg_new ;
