## Functions for data download and database handling

# Format string for lubridate to allow easy writing of human-readable update string to metadata
sf <- stamp("Updated Sunday, Jan 17, 1999 3:34")

openDB <- function(db){
  # Opens a connection to the app postgresql database
  #
  # Arg:
  #   db: A list of database 
  #
  # Returns:
  #   A list with:
  #    db$con: SQLiteConnection
  #    ...

  db$exception <- tryCatch(
    {
      db$con <- dbConnect(db$pgdrv, dbname = db$name, host = db$host, port = db$port, user = db$user, password = db$password)
      db$metadata <- list()
      db$status <- "OK"
    },
    error=function(cond) {
      db$message <- cond
      db$status <- "ERROR"
      return(db)
    },
    warning=function(cond) {
      db$message <- cond
      db$status <- "WARNING"
      return(db)
    },
    finally={
      # Additional processing
    }
  )   
  
  ### To do: additional database checking ###
  return(db)
}

closeDB <- function(db) {
  # Closes a connection to the app sqlite database
  # 
  # Arg:
  #   db:   A list object created by openDB, including a postgresql connection in db$con
  #
  # Returns: none
  dbDisconnect(db$con)
}

getGaugeSites <- function(db,metadata) {
  # Downloads raingauge site metadata from ECan and saves as a database table, "gauges"
  #
  # Arg:
  #   db:   A list object created by openDB, including a postgresql connection in db$con
  #
  # Returns: none
  #
  # ECan rainfall monitoring sites are available from:
  # http://opendata.canterburymaps.govt.nz/datasets/482291bb562540888b1aec7b85919827_5

  # GeoJSON site location data:
  gaugeJSON <- fromJSON("https://opendata.arcgis.com/datasets/482291bb562540888b1aec7b85919827_5.geojson", flatten = TRUE)
  timeofUpdate = now()
  
  # Extract gauge data as a data frame, rename columns to simplify and remove ".", convert to lowercase
  gauges <- data.frame(gaugeJSON$features) 
  colnames(gauges) <- str_replace_all(colnames(gauges),c("properties." = "", "geometry." = "geo_"))
  colnames(gauges) = dbSafeNames(colnames(gauges))

  # Remove rows with no samples. 
  gauges <- gauges %>% filter(!is.na(last_sample))
  
  # Convert last_sample to Date-Time and create a double representation
  gauges$last_sample <- gauges$last_sample %>% ymd_hms() #%>% force_tz(tzone = "Pacific/Auckland") %>% with_tz("UTC")
  gauges$last_sample_timestamp <- gauges$last_sample %>% as.double() # force_tz(tzone = "Pacific/Auckland") %>% as.double()
  
  # Convert point coordiates to real numbers and drop geo column #### POSTGIS: CHECK ON USING A SPATIALPOINTSDATAFRAME FOR GEO_COORDINATES
  geo <- pull(gauges,geo_coordinates) %>% unlist
  gauges$longitude <- geo[seq(1, length(geo), 2)]
  gauges$latitude <- geo[seq(2, length(geo), 2)]
  gauges <- gauges %>% select(-geo_coordinates)
 
  # Write to database table 
  dbWriteTable(db$con, "gauges", gauges, overwrite = TRUE)
  
  # Add row if needed and update values
  if (!str_detect(select(metadata,"data"),"gauges")) { metadata <- metadata %>% add_row(data = "gauges") }
  metadata <- metadata %>% 
    mutate(lastupdate = replace(lastupdate, data=="gauges", as.double(timeofUpdate))) %>%
    mutate(notes = replace(notes, data=="gauges", paste0(sf(timeofUpdate),' (',Sys.timezone(),')')))
  # Write back to database
  dbWriteTable(db$con, "metadata", metadata, overwrite = TRUE)
  
}

dbSafeNames = function(names) {
  # make names db safe: no '.' or other illegal characters,
  # all lower case and unique
  #
  names = gsub('[^a-z0-9]+','_',tolower(names))
  names = make.names(names, unique=TRUE, allow_=TRUE)
  names = gsub('.','_',names, fixed=TRUE)
  return(names)
}

loadGaugeSites <- function(db) {
  # Load gauge sites from the database and process into the app table

  #print("loadGaugeSites()")
  gauges <- dbReadTable(db$con,"gauges")
   
  return(gauges)
}
  
getSiteData <- function(siteID,timePeriod = "All") {
  # Get an invidual site's data for a requested period
  # 
  # Arg:
  #   siteID:     An integer identifier of the site
  #   timePeriod: The timeperiod to download (string). Default: "All"
  #
  # Returns: status code (0 = fail, 1 = success)
  
  print(paste0("Downloading: ", siteID))
  
  siteURL <- paste0("http://data.ecan.govt.nz/data/78/Rainfall/Rainfall%20for%20individual%20site/CSV?SiteNo=",siteID,"&Period=",timePeriod)

  # Download site data
  r <- httr::GET(siteURL)
  timeofUpdate = now()
  
  # Parse CSV string into table
  siteData <- read.table(text = content(r,"text"), sep =",", header = TRUE, stringsAsFactors = FALSE)
  
  # Handle empty sites: skip to next
  if (nrow(siteData) == 0) {
    print(paste0("  -> no data"))
    status <- 0
  } else {
  
    # Convert header names to lower case
    colnames(siteData) = dbSafeNames(colnames(siteData))
    
    # Process date strings into R date-times
    siteData$datetime <- gsub("[.]", "\\1", siteData$datetime) %>%
      strptime(., format = "%d/%m/%Y %I:%M:%S %p", tz = 'Pacific/Auckland') %>%
      as.POSIXct(.)
    
    # Add a timestamp in double format
    siteData$timestamp <- as.double(siteData$datetime)
    
    # Replace spuriously high values (>1000) with NA (the NZ 24-hour record is 758)
    siteData <- siteData %>% 
      mutate(rainfalltotal = replace(rainfalltotal, rainfalltotal > 1000, NA))
    
    #######################################################################
    ### TO DO
    #######################################################################
    # If All data downloaded, overwrite table
    dbWriteTable(db$con, as.character(siteID), siteData, overwrite = TRUE)
  
    # Otherwise, append, then remove duplicates
    
    # Handle hourly data - write to separate tables
    
    ## Write metadata
    if(!exists("metadata")){ # metadata is already loaded by the app - this check allows this function to run outside the app
      ## Get metadata table, or create if it doesn't exist ########
      if ("metadata" %in% dbListTables(db$con)) { metadata <- dbReadTable(db$con,"metadata")
      } else { metadata <- setNames(data.frame(matrix(ncol = 3, nrow = 0)), c("data", "lastupdate", "notes")) }
    }
    # Add row if needed and update values
    if (!str_detect(select(metadata,"data"),as.character(siteID))) { metadata <- metadata %>% add_row(data = as.character(siteID)) }
    metadata <- metadata %>% 
      mutate(lastupdate = replace(lastupdate, data==as.character(siteID), as.double(timeofUpdate))) %>%
      mutate(notes = replace(notes, data==as.character(siteID), paste0(sf(timeofUpdate),' (',Sys.timezone(),')')))
    # Write back to database
    dbWriteTable(db$con, "metadata", metadata, overwrite = TRUE)

    status <- 1
  }
  return(status)
}

loadSiteData <- function(SiteID,db) {
  db <- openDB(db)
  if (is.na(db$status)) { 
    showNotification(ui = "Unable to connect to database.", type = "error") 
    return(NULL)
  }

  data <- dbReadTable(db$con,as.character(SiteID))
  dbDisconnect(db$con)
  return(data)
}

loadIDF <- function(SiteID,db) {
  db <- openDB(db)
  if (is.na(db$status)) { 
    showNotification(ui = "Unable to connect to database.", type = "error") 
    return(NULL)
  }

  data <- dbReadTable(db$con,paste0(as.character(SiteID),"_IDF"))
  dbDisconnect(db$con)
  return(data)
}

updateData <- function(db) {
  # Updates all data, downloading using getSiteData()
  # 
  # Arg:
  #   db:   A list object created by openDB, including a postgresql connection in db$con
  #
  # Returns: none
  
  gauges <- dbReadTable(db$con,"gauges")
  tblList <- dbListTables(db$con)
  
  # Process for all sites
  for (siteID in gauges$sitenumber) {
    getSiteData(siteID, timePeriod = "All")
  }

}

loadSpatial <- function() {
  # Load spatial data needed and insert into database
  #
  # Args:
  #   datalist: list of data files to load
  #   db:       database connection list including db$con specifying open database
  #
  # Returns: None
  
  spatial <- list()
  
  #dataJSON <- FROM_GeoJson(url_file_string = "https://datafinder.stats.govt.nz/services/query/v1/vector.json?key=6a8ce216ef5f4228b528c583aff0b7f4&layer=98763&x=172.6332999999969&y=-43.53330000000021&max_results=3&radius=10000&geometry=true&with_field_names=true")
  spatial$regionalCouncil <- readOGR("regional-council-2019-generalised.gpkg")
  spatial$territorialAuthority <- readOGR("territorial-authority-2019-generalised.gpkg")

  return(spatial)
} 

processSpatial <- function(spatial,gauges) {
  
  lat <- gauges$latitude
  lon <- gauges$longitude
  dataCRS <- CRS("+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0")
  
  spatial$gauges <- SpatialPointsDataFrame(cbind(lon,lat), proj4string=dataCRS, data = gauges)
   
  o = over(spatial$gauges, spatial$regionalCouncil)
  o <- o %>% select('REGC2019_V1_00_NAME')
  colnames(o) <- "region"
  spatial$gauges@data = cbind(spatial$gauges@data, o)

  o = over(spatial$gauges, spatial$territorialAuthority)
  o <- o %>% select('TA2019_V1_00_NAME')
  colnames(o) <- "territory"
  spatial$gauges@data = cbind(spatial$gauges@data, o)
  
  return(spatial)
}

queryDate <- function(db,gauges,getDate = today()) {
  db <- openDB(db)
  getDate <- as.Date(getDate)
  fromDate <- as.Date(getDate)-7
  gaugesThisDate <- gauges[0, ] # empty data frame with same columns as gauges
  for (siteID in gauges$sitenumber) {

    gaugeData <- tbl(db$con, as.character(siteID)) %>%
      filter(datetime <= getDate & datetime >= fromDate) %>%
      collect()

    thisGauge <- gauges %>% filter(sitenumber == siteID)
    if (nrow(gaugeData) == 0) {
      thisGauge[c("rain_today", "rain_1_day_ago", "rain_2_days_ago", "rain_3_days_ago", "rain_4_days_ago", "rain_5_days_ago", "rain_6_days_ago", "rain_7_days_ago", "total_rainfall")] <- NA
    } else {
      gaugeData <- gaugeData %>% mutate(datetime = as.Date(datetime))
      thisGauge["rain_today"] <- queryDate_GetVal(gaugeData,getDate)
      thisGauge["rain_1_day_ago"] <- queryDate_GetVal(gaugeData,getDate-1)
      thisGauge["rain_2_days_ago"] <- queryDate_GetVal(gaugeData,getDate-2)
      thisGauge["rain_3_days_ago"] <- queryDate_GetVal(gaugeData,getDate-3)
      thisGauge["rain_4_days_ago"] <- queryDate_GetVal(gaugeData,getDate-4)
      thisGauge["rain_5_days_ago"] <- queryDate_GetVal(gaugeData,getDate-5)
      thisGauge["rain_6_days_ago"] <- queryDate_GetVal(gaugeData,getDate-6)
      thisGauge["rain_7_days_ago"] <- queryDate_GetVal(gaugeData,getDate-7)
      thisGauge["total_rainfall"] <- queryDate_intervalSum(gaugeData,fromDate,getDate)
    }
    gaugesThisDate <- gaugesThisDate %>% rbind(thisGauge)
    
    
  }
  # Update probabilities for this date:
  gaugesThisDate <- getRainProbability(gaugesThisDate,db,writetoDB = FALSE)
  
  dbDisconnect(db$con)
  
  return(gaugesThisDate)
}
queryDate_GetVal <- function(gaugeData,thisDate) {
  gaugeData %>%
    filter(datetime == thisDate) %>%
    pull(rainfalltotal)
}
queryDate_intervalSum <- function(gaugeData,fromDate,toDate) {
  gaugeData %>%
    filter(datetime <= toDate & datetime > fromDate) %>%
    pull(rainfalltotal) %>% sum() 
}

generateCleantable <- function(gauges) {
  # Produce a clean version of the gauges dataframe for the table display
  cleantable <- gauges %>%
    select(Gauge = sitenumber,
      Name = sitename,
      Catchment = river,
      Region = region,
      District = territory,
      LastSample = last_sample,
      RainHour = last_hour,
      RainDay = rain_today,
      RainWeek = total_rainfall,
      Altitude = altitude,
      Lat = latitude,
      Lon = longitude
    )
  
  return(cleantable)
}



