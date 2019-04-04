## Functions for statistical calculations on rain data


getRainProbability <- function(gauges,db,writetoDB = TRUE) {
  # Get the rainfall exceedance probability for the current observations

  v <- c(rain_today = "rain_today_prob",
         rain_1_day_ago = "rain_1_day_ago_prob",
         rain_2_days_ago = "rain_2_days_ago_prob",
         rain_3_days_ago = "rain_3_days_ago_prob",
         rain_4_days_ago = "rain_4_days_ago_prob",
         rain_4_days_ago = "rain_4_days_ago_prob",
         rain_5_days_ago = "rain_5_days_ago_prob",
         rain_6_days_ago = "rain_6_days_ago_prob",
         rain_7_days_ago = "rain_7_days_ago_prob",
         total_rainfall = "total_rainfall_prob")
  
  for (i in v) gauges[i] <- NA  # create empty column - avoids a problem when regenerating the table
  
  gaugesFitD1 <- dbReadTable(db$con, "GaugesFit_D1")
  colnames(gaugesFitD1) <- c("sitenumber", "D1_location","D1_scale","D1_type")
  gaugesFitD7 <- dbReadTable(db$con, "GaugesFit_D7")
  colnames(gaugesFitD7) <- c("sitenumber", "D7_location","D7_scale","D7_type")
  
  raindataFit <- left_join(gauges, gaugesFitD1, by = "sitenumber")
  raindataFit <- left_join(raindataFit, gaugesFitD7, by = "sitenumber")
  
  # Remove NAs and keep site number as index
  raindataFit <- raindataFit %>% filter(!is.na(D1_scale))
  rainProbs <- raindataFit %>% select(sitenumber)
  
  for (n in names(v)) {
    # Estimate exceedance probability of given duration

    # Extract vectors for easier readability
    rainvals <- raindataFit[[n]]
    if (n == "total_rainfall") {
      scale <- raindataFit[["D7_scale"]]
      loc <- raindataFit[["D7_location"]]
    } else {
      scale <- raindataFit[["D1_scale"]]
      loc <- raindataFit[["D1_location"]]
    }
    
    # Calculate probability for all sites for this observation series
    rainProbs[v[[n]]] <- mapply(pevd, rainvals, loc = loc, scale = scale, type = "Gumbel", lower.tail = FALSE)

  }
  # Join back to gauges data and write to database (dropping prob columns first)
  gauges <- gauges %>% 
    select(-v) %>%
    left_join(rainProbs, by = "sitenumber")
  if (writetoDB) dbWriteTable(db$con, "gauges", gauges, overwrite = TRUE)
  
  return(gauges)
}


calcIntensities <- function(siteData,durations = c(2,3,4,5,6,7,10,15,20,30)) {
  # Obtain rolling sums of the daily rainfall 
  for (d in durations) {
    cName <- paste0('Sum_',d)
    siteData <- siteData %>% 
      mutate(!!cName := roll_sum(rainfalltotal, d, align = "right", fill = NA))
  }
  return(siteData)
}

calcAnnualMax <- function(siteData, Durations = c(1,2,3,4,5,6,7,10,15,20,30)) {
  # Get a table of the annual maxima for each period
  annualMax <- siteData %>% group_by(year = year(with_tz(as_datetime(siteData$timestamp),tzone = "Pacific/Auckland"))) %>% 
    summarize(max_1 = max(rainfalltotal,na.rm = TRUE),
              max_2 = max(Sum_2,na.rm = TRUE),
              max_3 = max(Sum_3,na.rm = TRUE),
              max_4 = max(Sum_4,na.rm = TRUE),
              max_5 = max(Sum_5,na.rm = TRUE),
              max_6 = max(Sum_6,na.rm = TRUE),
              max_7 = max(Sum_7,na.rm = TRUE),
              max_10 = max(Sum_10,na.rm = TRUE),
              max_15 = max(Sum_15,na.rm = TRUE),
              max_20 = max(Sum_20,na.rm = TRUE),
              max_30 = max(Sum_30,na.rm = TRUE))
  
  # Take care of any -Inf values caused by insufficient data - convert to NA
  for (D in Durations) {
    annualMax[[paste0("max_",D)]] <- replace(annualMax[[paste0("max_",D)]],!is.finite(annualMax[[paste0("max_",D)]]),NA)
  }

  return(annualMax)
}

fitGEV <- function(annualMax, method = "MLE", type = "Gumbel", Durations = c(1,2,3,4,5,6,7,10,15,20,30)) {
  # Fit a GEV to each annual maxima series
  Fit <- list()
  for (D in Durations) {
    Fit[[paste0("max_",D)]] <- tryCatch({ # For timeseries that are too short to fit a distribution, NA will be returned
      Fit[[paste0("max_",D)]] <- fevd(annualMax[[paste0("max_",D)]],type = type, method = method, na.action = na.omit)  
    }, condition = function(c) {
      Fit[[paste0("max_",D)]] <- NA
    })
  }

  return(Fit)
}

calcIDF <- function(Fit, T = c(5,10,25,50,100)) {
  # Get inverse estimates of rainfall levels for multiple return periods - i.e. the IDF curves
  
  for (n in names(Fit)) {
      if (!is.list(Fit[[n]])) { return(NA) }
  }

  idf <- data.frame()
  idf <- idf %>% 
    rbind(return.level(Fit$max_1,T)) %>%
    rbind(return.level(Fit$max_2,T)/2) %>%
    rbind(return.level(Fit$max_3,T)/3) %>%
    rbind(return.level(Fit$max_4,T)/4) %>%
    rbind(return.level(Fit$max_5,T)/5) %>%
    rbind(return.level(Fit$max_6,T)/6) %>%
    rbind(return.level(Fit$max_7,T)/7) %>%
    rbind(return.level(Fit$max_10,T)/10) %>%
    rbind(return.level(Fit$max_15,T)/15) %>%
    rbind(return.level(Fit$max_20,T)/20) %>%
    rbind(return.level(Fit$max_30,T)/30)
  colnames(idf) <- paste0("T",T)
  idf <- idf %>% mutate(Duration = c(1,2,3,4,5,6,7,10,15,20,30))

  return(idf)
}

gaugesStats <- function(db, Durations = c(1,2,3,4,5,6,7,10,15,20,30)) {
  # Calculate stats for all gauges
  
  gauges <- dbReadTable(db$con,"gauges")

  # Set up output tables for saving model fits
  gaugesFit <- list()
  for (D in Durations) {
    gaugesFit[[paste0('D',D)]] <- setNames(data.frame(matrix(ncol = 4, nrow = 0)), c("sitenumber", "location", "scale", "type"))
  }

  # Get stats for each site
  for (siteID in gauges$sitenumber) {

    # Calculate intensities, get annual maxima, fit Gumbel and calculate the IDF
    siteData <- dbReadTable(db$con, as.character(siteID))
    siteData <- calcIntensities(siteData)  
    annualMax <- calcAnnualMax(siteData)
    Fit <- fitGEV(annualMax)
    idf <- calcIDF(Fit)
    
    # Save IDF to database
    if (is.data.frame(idf)) {
      dbWriteTable(db$con, paste0(siteID,"_IDF"), idf, overwrite = TRUE)
    }
    
    # For each Fit for each duration, extract and save the parameters for this site
    for (D in Durations) {
      if (is.data.frame(idf)) {
        plist <- distill.fevd(Fit[[paste0('max_',D)]])
        gaugesFit[[paste0('D',D)]] <- gaugesFit[[paste0('D',D)]]%>% 
          add_row(sitenumber = siteID, location = plist[['location']], scale = plist[['scale']], type = Fit[[paste0('max_',D)]]$type)
      } else {
        gaugesFit[[paste0('D',D)]] <- gaugesFit[[paste0('D',D)]]%>% 
          add_row(sitenumber = siteID, location = NA, scale = NA, type = NA)
      }
    }
    
  }

  # Write completed gaugesFit tables to database
  for (D in Durations) {
    dbWriteTable(db$con, paste0("GaugesFit_D",D), gaugesFit[[paste0('D',D)]], overwrite = TRUE)
  }
  return(gaugesFit)

}






