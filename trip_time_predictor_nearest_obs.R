
library(tidyverse)
library(lubridate)
library(anytime)


#trip_maker function
trip_maker <- function(x){
  
  a <- x %>% 
    filter(FIRST_closer_LAST == 1)%>%
    select(active_tripid,
           vehicle_id, 
           stop_name,
           next_stop,
           arrivalTime, 
           stop_lat,
           stop_lon, 
           update_time = lead_first_update_time, 
           latitude = lead_first_latitude,
           longitude = lead_first_longitude,
           dist_to_stop = dist_to_stop_FIRST,
           FIRST_closer_LAST)
  
  b <- x%>%
    filter(FIRST_closer_LAST == 0)%>%
    select(active_tripid,
           vehicle_id, 
           stop_name,
           next_stop,
           arrivalTime, 
           stop_lat,
           stop_lon, 
           update_time = lastL_update_time, 
           latitude = lastL_latitude,
           longitude = lastL_longitude,
           dist_to_stop = dist_to_stop_LAST,
           FIRST_closer_LAST)
  
  
  bind_rows(a, b)%>%
    arrange(update_time)%>%
    group_by(active_tripid, stop_name,  next_stop, arrivalTime, stop_lat, stop_lon)%>%
    arrange(update_time)%>%
    filter(row_number() == 1)%>%
    ungroup()%>%
    mutate(lead_latitude = lead(latitude),
           lead_longitude = lead(longitude),
           lead_update_time = lead(update_time),
           time_diff = lead_update_time - update_time,
           dist_obs = abs(abs(lead_longitude) - abs(longitude)) + abs(abs(lead_latitude) - abs(latitude)),
           speed = dist_obs/as.numeric(time_diff),
           speed = if_else(is.na(speed), median(speed, na.rm = T), speed),
           extime = dist_to_stop/speed,
           extime = if_else(FIRST_closer_LAST == 0 , extime*1, extime*-1),
           update_time = if_else(dist_to_stop >= 0.0015, update_time + extime, update_time)) ### cut of approx 150 meters which is about 0.0015 abs degrees or less for prediction
  
}  



########################## TStreetcar Trip Time Between Stops ########################################

options(digits = 13)

####Days I have data for ### started on 08/14 but no only got partial day##### (for some reason no 08/21 data bet its in the 1970 day some api problems perhaps)
#1970-01-01 2019-08-14 2019-08-15 2019-08-17 2019-08-18 2019-08-19 2019-08-20 2019-08-22 2019-08-23 2019-08-24 2019-08-25 
#7541       1391       3140       3927       3458       2906       2258       3790       3040       3175       3407 

run_date = '2019-08-25'

#read in transit data 
s_car <- read_csv("D:/Transit Data/streetcar_data.csv")%>%
  mutate(last_local_update = anytime(as.numeric(str_sub(last_local_update, end = -4))),
         last_update_time  = anytime(as.numeric(str_sub(last_update_time, end = -4))),
         service_date = as.Date(anytime(as.numeric(str_sub(service_date, end = -4)))))%>%
  filter(service_date == run_date)%>%
  filter(last_local_update < as.Date(run_date) + 1)%>% ##getting rid of trips that started in target day but ran into next day
  filter(last_update_time < as.Date(run_date) + 1) ##getting rid of trips that started in target day but ran into next day


s_car_trips <- unique(s_car$active_tripid)


scar_stops <- read_csv("D:/Transit Data/streetcar_stop_schedules.csv") %>%
  select(-c("serviceDate", "service_Date", "route_id"))


#########   Streetcar Predicted Arrival Time   ############

list1 <- list()

for(i in 1:length(s_car_trips)){
  
  list1[[i]] <- left_join(s_car, scar_stops, by = c('active_tripid'= 'tripid', 'next_stop' = 'stop_id'))%>% 
    arrange(active_tripid, arrivalTime, last_update_time)%>%
    select(active_tripid, vehicle_id, block_id, service_id, direction_id, route_id, last_latitude, last_longitude, last_update_time, last_local_update, arrivalTime, stop_name, next_stop, stop_lat, stop_lon)%>%
    filter(active_tripid == s_car_trips[i]) %>% #this line will be mremoved later on (this route went back to a stop twice  first time was ontime second time was late need to see if other trips do this or just because end of day)
    group_by(active_tripid, vehicle_id,  stop_name, next_stop) %>%
    arrange(last_update_time)%>%
    summarise(lastL_update_time = last_update_time[length(last_update_time)], 
              lastL_local_update = last_local_update[length(last_local_update)],
              lastL_latitude = last_latitude[length(last_latitude)],
              lastL_longitude = last_longitude[length(last_longitude)],
              arrivalTime = arrivalTime[length(arrivalTime)], 
              stop_lat = stop_lat[length(stop_lat)], 
              stop_lon = stop_lon[length(stop_lon)],
              first_update_time = last_update_time[1],
              first_local_update = last_local_update[1],
              first_latitude = last_latitude[1],
              first_longitude = last_longitude[1])%>%
    arrange(lastL_update_time)%>%
    ungroup()%>%
    arrange(lastL_update_time)%>%
    mutate(dist_to_stop_LAST = abs(abs(stop_lon) - abs(first_longitude)) + abs(abs(stop_lat) - abs(first_latitude)))%>% 
    mutate(lead_first_update_time = lead(first_update_time),
           lead_first_latitude = lead(first_latitude),
           lead_first_longitude = lead(first_longitude))%>%
    mutate(dist_to_stop_FIRST = abs(abs(stop_lon) - abs(lead_first_longitude)) + abs(abs(stop_lat) - abs(lead_first_latitude)),
           FIRST_closer_LAST = if_else(dist_to_stop_FIRST <= dist_to_stop_LAST, 1 , 0))%>%
    impute(0, FIRST_closer_LAST)%>%
    trip_maker()
  
}




bind_rows(list1)%>% write_csv(paste0('D:/Transit Data/Street Car Data Processed 081519/Street_Car_',as.Date(list1[[1]]$update_time[1]), '.csv'))



####need to take the processed data and put in the RAW format so column names and definitions are clear then need to build file splitter#######

