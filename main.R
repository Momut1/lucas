library(lucas)
library(RPostgreSQL)
library(utils)

pg_hostname<-'localhost'
pg_user<-'postgres'
pg_password<-'postgres'
pg_dbname<-'postgres'
pg_port<-'5432'

# Set the root dir ----
if(.Platform$OS.type == "unix") {root1<-'/'}
if (.Platform$OS.type == "windows") { root1<-'C:/'}

#Create directory tree
working_dir<-paste0(root1,'data/LUCAS_harmo')
data_dir <- file.path(working_dir, 'data')
output_dir<-file.path(data_dir,'output')
input_dir<-file.path(data_dir,'input')
#input_dir<-file.path(working_dir,'data/input/Old_data')
mappings_csv_folder<-file.path(data_dir,"mappings")
support_dir <- file.path(data_dir, 'supportDocs')
#mappings_csv_folder<-system.file("extdata", "mappings", package = "lucas")

#create dirs if not created
dirs <- c(working_dir, data_dir, output_dir, input_dir, mappings_csv_folder, support_dir)
for(i in dirs){
  if(!dir.exists(i)){
    dir.create(i, recursive = TRUE)
  }
}


#download data JRCFTP
## requires lftp client !!!
linkToLUCASftp <- 'https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/LUCAS/LUCAS_harmonised/'
fileTree <- system(paste('lftp -e "find;quit"', linkToLUCASftp), intern = T)
fileTree <- fileTree[-1]
if(!dir.exists(file.path(working_dir,'LUCAS_harmonized_FromRepo'))){
  dir.create(file.path(working_dir,'LUCAS_harmonized_FromRepo'))
}

for(i in 1:length(fileTree)){
  repoFile <- paste0(strsplit(fileTree[i], '/')[[1]][2:length(strsplit(fileTree[i], '/')[[1]])], sep='/', collapse = '')
  repoFile <- substr(repoFile, 1,nchar(repoFile)-1)
  if(!grepl('.', repoFile, fixed = T)){
    if(!dir.exists(file.path(working_dir, 'LUCAS_harmonized_FromRepo', repoFile))){
      dir.create(file.path(working_dir, 'LUCAS_harmonized_FromRepo', repoFile))
    }
  }
  if(grepl('.', repoFile, fixed = T)){
    download.file(paste0(linkToLUCASftp, repoFile), destfile = file.path(working_dir,'LUCAS_harmonized_FromRepo', repoFile), quiet = F)
  }
}

#copy data to respective folder
for(filee in list.files(file.path(working_dir, 'LUCAS_harmonised', '4_mappings'))){
  file.copy(file.path(working_dir, 'LUCAS_harmonised', '4_mappings',filee), mappings_csv_folder)
}

for(filee in list.files(file.path(working_dir, 'LUCAS_harmonised', '3_supporting'))){
  file.copy(file.path(working_dir, 'LUCAS_harmonised', '3_supporting',filee), support_dir)
}


# 0/ Connect to PG DB and set up directories (input, output, mappings) ----
con <- Connect_to_db(pg_user, pg_hostname,pg_port,pg_password,pg_dbname) # Connect to the PG db where you want to upload all LUCAS points
#dbSendQuery(con,"CREATE EXTENSION postgis;") # if not installed


# 1/ Upload all the raw LUCAS survey data to the DB without modifying them ----
Upload_to_db(input_dir, con) # Upload to the DB all the 2009-2018 lucas csv
#Upload_exif(con,file.path(mappings_csv_folder,'lucas_exif.csv'))

# 2/ Harmonisation of each table separately to match the structure of the 2018 Table ----
Rename_cols(con, file.path(mappings_csv_folder,'columnRename.csv')) # Rename columns to match 2018 survey
Add_photo_fields_2006(con)
Add_missing_cols(con, c(2006, 2009, 2012, 2015, 2018)) # Adds missing columns to all tables before merge
Add_new_cols(con, c(2006, 2009, 2012, 2015, 2018)) # Adds new columns to all table that will be necessary for when tables are merged.
Upper_case(con,c(2009, 2012, 2015, 2018)) # Convert values in designated columns (lc1, lc1_spec, lu1, lu1_type, lc2, lc2_spec, lu2, lu2_type, cprn_lc) to uppercase for consistency's sake
Recode_vars(con, file.path(mappings_csv_folder,'RecodeVars.csv'), c(2006, 2009, 2012, 2015, 2018), c('lc1_perc', 'lc2_perc', 'lu1_perc', 'lu2_perc', 'soil_stones_perc')) # Updates values in all tables to fit the last survey (2018) in terms of the coding of different variables; update is based on pre-made mappings
Order_cols(con, c(2006, 2009, 2012, 2015)) # Changes order of columns to fit the last survey (2018) and set all column data type to character varying in order to prepare for merge

# 3 / Merge the 5 harmonised tables to one unique table lucas_harmo_pack ----
Merge_harmo(con, file.path(support_dir,'LUCAS_harmo_RD.csv')) # Merge all tables into a single harmonized version containing all years and change to relevant data type, as mapped in the record descriptor

# 4 / Post-processing (consistency check, add geometries, add index, add revisit, add legend explicit field) and creation of lucas_harmo_pack_uf ----
Consistency_check(con, c(2006, 2009, 2012, 2015, 2018), file.path(mappings_csv_folder, 'manChangedVars.csv')) # Perform consistency checks on newly created tables to ensure conformity in terms of column order and data types
Correct_th_loc(con, file.path(input_dir, 'GRID_CSVEXP_20171113.csv'))
Add_geom(con) # Add geometries and calculated distance :location of theoretical point(th_geom), ocation of lucas survey (gps_geom), lucas transect geometr (trans_geom) and distance between theoretical and survey point (th_gps_dist)
Create_tags(con) # Create database tags (primary key), index, and spatial index and a new id column for the harmonized table
Add_revisit(con) # Adds revisit column to lucas harmonized table to show the number of times between the years when the point was revisited.
Allign_Map_CSVs(mappings_csv_folder, c(2006, 2009, 2012, 2015, 2018)) #Allign mappings CSV to fix incosistencies between years
Check_Map_CSVs(mappings_csv_folder, c(2006, 2009, 2012, 2015, 2018), file.path(support_dir,'C3_legends_new.csv'))
User_friendly(con, mappings_csv_folder, c(2006, 2009, 2012, 2015, 2018)) # Creates columns with labels for coded variables and decodes all variables where possible to explicit labels

#TODO: add here a geometry check with the NUTS
#Fill_nuts0(con) # Fill the countries on the nut0 field for the rows with missin data

UF_Consistency_check(con) # Perform consistency checks on newly created UF fields to ensure conformity in terms of column order and data types
Final_order_cols(con) # Re-order columns of final tables
Remove_vars(con, c('lndmng_plough', 'lm_plough_slope', 'lm_plough_direct', 'th_geom', 'gps_geom', 'trans_geom'))
Update_rd(con, file.path(support_dir,'LUCAS_harmo_RD.csv'), c(2006, 2009, 2012, 2015, 2018))

# 5 /  Export geometries to shapefile: LUCAS_gps_geom, LUCAS_th_geom and LUCAS_th_geom ----
dir.create(paste0(output_dir,'geometry'), recursive = T)
# export gps_geom
dir.create(paste0(output_dir,'geometry/LUCAS_gps_geom'), recursive = T)
system(paste('pgsql2shp', '-f',
             paste0(output_dir,'geometry/LUCAS_gps_geom','/LUCAS_gps_geom.shp'),
             '-h',pg_hostname, '-u',pg_user,'-P',pg_password,  pg_dbname,
             '"SELECT id, point_id, year,  gps_geom FROM lucas_harmo_pack WHERE gps_geom IS NOT NULL;"'
))
zip(zipfile = paste0(output_dir,'geometry/LUCAS_gps_geom','.zip'), files = paste0(output_dir,'geometry/LUCAS_gps_geom'),extras = '-j')
dir.rm(paste0(output_dir,'geometry/LUCAS_gps_geom'))
unlink(paste0(output_dir,'geometry/LUCAS_gps_geom'), recursive=TRUE)

# # epxort th_geom
# dir.create(paste0(output_dir,'geometry/LUCAS_gps_geom'), recursive = T)
# system(paste('pgsql2shp', '-f',
#              paste0(output_dir,'geometry/LUCAS_gps_geom','/LUCAS_gps_geom.shp'),
#              '-h',pg_hostname, '-u',pg_user,'-P',pg_password,  pg_dbname,
#              '"SELECT id, point_id, year, gps_geom FROM lucas_harmo_pack WHERE gps_geom IS NOT NULL;"'
# ))
# zip(zipfile = paste0(output_dir,'geometry/LUCAS_gps_geom','.zip'), files = paste0(output_dir,'geometry/LUCAS_gps_geom'),extras = '-j')
# unlink(paste0(output_dir,'geometry/LUCAS_gps_geom'), recursive=TRUE)

# epxort th_geom
dir.create(paste0(output_dir,'geometry/LUCAS_th_geom'), recursive = T)
system(paste('pgsql2shp', '-f',
             paste0(output_dir,'geometry/LUCAS_th_geom','/LUCAS_th_geom.shp'),
             '-h',pg_hostname, '-u',pg_user,'-P',pg_password,  pg_dbname,
             '"SELECT id, point_id, year, th_geom FROM lucas_harmo_pack WHERE th_geom IS NOT NULL;"'
))
zip(zipfile = paste0(output_dir,'geometry/LUCAS_th_geom','.zip'), files = paste0(output_dir,'geometry/LUCAS_th_geom'),extras = '-j')
unlink(paste0(output_dir,'geometry/LUCAS_th_geom'), recursive=TRUE)


# epxort trans_geom
dir.create(paste0(output_dir,'geometry/LUCAS_trans_geom'), recursive = T)
system(paste('pgsql2shp', '-f',
             paste0(output_dir,'geometry/LUCAS_trans_geom','/LUCAS_trans_geom.shp'),
             '-h',pg_hostname, '-u',pg_user,'-P',pg_password,  pg_dbname,
             '"SELECT id, point_id, year,  transect, trans_geom FROM lucas_harmo_pack WHERE transect IS NOT NULL;"'
))
zip(zipfile = paste0(output_dir,'geometry/LUCAS_trans_geom','.zip'), files = paste0(output_dir,'geometry/LUCAS_trans_geom'),extras = '-j')
unlink(paste0(output_dir,'geometry/LUCAS_trans_geom'), recursive=TRUE)


.
# 5 /  Export harmo table as CSV ----
dir.create(paste0(output_dir,'table'), recursive = T)
q <-dbSendQuery(con, "SELECT * FROM lucas_harmo_pack_uf_final;")
lucas_harmo_uf <- fetch(q,n = Inf)
drops <- c('th_geom', 'gps_geom', 'trans_geom')
lucas_harmo_uf<-lucas_harmo_uf[,!(names(lucas_harmo_uf) %in% drops)] # remove the geometries
write.csv(lucas_harmo_uf,paste0(output_dir,'table/lucas_harmo_uf.csv'), row.names = F)
zip(zipfile = paste0(output_dir,'table/lucas_harmo_uf','.zip'), files = paste0(output_dir,'table/lucas_harmo_uf.csv'),extras = '-j')
unlink(paste0(output_dir,'table/lucas_harmo_uf.csv'), recursive=TRUE)

# 6 /  Export harmo table as CSV per year  ----
drops <- c('th_geom', 'gps_geom', 'trans_geom')
lucas_harmo_uf<-lucas_harmo_uf[,!(names(lucas_harmo_uf) %in% drops)] # remove the geometries
for  (year in c(2006, 2009, 2012, 2015,2018)) {
  lucas_harmo_uf_year<-lucas_harmo_uf[lucas_harmo_uf$year==year,] # remove the geometries
  nrow(lucas_harmo_uf_year)
  write.csv(lucas_harmo_uf_year,file.path(output_dir,'table',paste0('lucas_harmo_uf_',year,'.csv')), row.names = F)
  zip(zipfile = file.path(output_dir,'table',paste0('lucas_harmo_uf_',year,'.zip')), files = file.path(output_dir,'table',paste0('lucas_harmo_uf_',year,'.csv')),extras = '-j')
  unlink(file.path(output_dir,'table',paste0('lucas_harmo_uf_',year,'.csv')), recursive=TRUE)
}

# 7 / Export support documents to zip ----
docs <- list.files(mappings_csv_folder)
docs <- docs[- which(docs=="LUCAS_harmo_RD.csv")]

if(! dir.exists(file.path(data_dir, 'supportDocs'))){
  dir.create(file.path(data_dir, 'supportDocs'))
}
for(doc in docs){
  file.copy(file.path(mappings_csv_folder, doc), file.path(data_dir, 'supportDocs', doc))
}
zip(file.path(data_dir, 'supportDocs',".zip"), files=paste(mappings_csv_folder, docs, sep="/"))

