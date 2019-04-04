##### Rainshiny

# Dates to check out in the app:
# 20 Feb 2018 - Hurricane Gita. 
# 21 Jul 2017 - Canterbury and Christchurch flooding - see http://floodlist.com/australia/new-zealand-otago-canterbury-july-2017

## To do for developments:
# Background database updates
# Calculate SPI
# 3-month rainfall
# Add isohyetal lines to map - check box to select
# Create database tables of each year/date/gauge so that the date query is faster
# Fix 180 meridian mapping issue (Chatham Is. data) - add 360 to all negative longitude

# Code released under the MIT license


library(shiny)
library(tidyverse)
library(leaflet)
library(RColorBrewer)
library(scales)
library(lattice)
#library(dplyr)
library(dbplyr)
library(spdplyr)
library(DT)
library(jsonlite)
#library(stringr)
library(DBI)
library(rgdal)
#library(readr)
library(httr)
library(RPostgreSQL)
library(lubridate)
library(sp)
#library(geojsonR)
library(extRemes)
library(RcppRoll)
library(reshape2)

#library(pandas)
library(viridis)

library(ggplot2)
theme_set(theme_minimal(base_size = 14))

# Load in data and stats functions
source('datahandling.R')
source('statsfunctions.R')
set.seed(seed = NULL)

aboutText <- 
  "<h3>Rainshiny</h3>
  <p>Version 0.1 (3-Apr-2019)</p>
  <p>R Shiny app which demonstrates near-real time statistical analysis of rain gauge data.</p>
  <p>This app is available for download under the MIT license from Github at <a href=/'https://github.com/MatthewDWilson/rainshiny/'>https://github.com/MatthewDWilson/rainshiny</a>
  <h4>Data providers</h4>
  <p>All data presented are copyright of the respective owners and they are gratefully acknowledged:</p>
  <ul>
    <li><a href=/'https://www.ecan.govt.nz/data/rainfall-data//'>Environment Canterbury</a>. Data license: <a href=/'http://creativecommons.org/licenses/by/3.0/nz//'>Creative Commons Attribution 3.0 New Zealand</a>.</li>
  </ul>
  <p>&nbsp;</p>
  <h4>App version history</h4>
  <ul>
    <li>0.1   : 2019-04-03 : Initial Beta release</li>
  </ul>
"

# Connect to the Postgres database
# Edit for production env:
db <- list()
db$pgdrv <- dbDriver(drvName = "PostgreSQL")
db$name <- "postgres"
db$host <- "172.17.0.2"
db$port <- 5432
db$user <- "postgres"
db$password <- "raintoday"
db$con <- NA

# Set up empty global variables to handle app data
rain <- list() # The main rainfall data
rain$selectedGauge <- NULL
metadata <- data.frame()
spatial <- list() # Additional spatial data
updateFreq <- 12 * 3600 # time between data updates, in seconds

vars <- c(
  "Total over last day" = "rain_today",
  "Total over last 7 days" = "total_rainfall"
)

# Define UI for application
ui <- navbarPage("Rainshiny", id="nav",
                 
                 tabPanel("Map",
                          div(class="outer",
                              
                              tags$head(
                                # Include our custom CSS
                                includeCSS("styles.css"),
                                includeScript("gomap.js")
                              ),
                              
                              # If not using custom CSS, set height of leafletOutput to a number instead of percent
                              leafletOutput("map", width="100%", height="100%"),
                              
                              # Shiny versions prior to 0.11 should use class = "modal" instead.
                              absolutePanel(id = "controls", class = "panel panel-default", fixed = TRUE,
                                            draggable = TRUE, top = "auto", left = "auto", right = 20, bottom = 20,
                                            width = 450, height = "auto",
                                            
                                            h4("Select data"),
                                            dateInput('selectedDate', label = 'Date', value = today(), min = "2004-12-31", max = today(), format ="dd/mm/yyyy", startview = "month", weekstart = 0, width = NULL),
                                            selectInput("type", "Rain stat to map:", vars, selected = "rain_today"),
                                            plotOutput("histRain", height = 150),
                                            selectInput("site", "Selected gauge:", list()),
                                            plotOutput("timeSeries", height = 200),
                                            plotOutput("IDFplot", height = 200)
                              ),
                              
                              tags$div(id="cite",
                                       'Data credit: ', tags$em('Environment Canterbury'), ', https://www.ecan.govt.nz/data/rainfall-data/.'
                              )
                          )
                 ),
                 
                 tabPanel("Data table",
                          fluidRow(
                            column(3,
                                  dateInput('selectedDateTable', label = 'Date', value = today(), min = "2004-12-31", max = today(), format ="dd/mm/yyyy", startview = "month", weekstart = 0, width = NULL)
                            )
                          ),
                          fluidRow(
                            column(4,
                                   selectInput("region", "Region", choices = c("Canterbury Region","Area Outside Region"), multiple=TRUE) # state
                            ),
                            column(4,
                                   conditionalPanel("input.region", selectInput("district", "District", c("All districts"=""), multiple=TRUE) # cities
                                   )
                            ),
                            column(4,
                                   conditionalPanel("input.region", selectInput("sitecodes", "Gauge", c("All sites"=""), multiple=TRUE)
                                   )
                            )
                          ),
                          fluidRow(
                            column(2,
                                   numericInput("minRain", "Min rain (mm)", min=0, max=NA, value=0)
                            ),
                            column(2,
                                   numericInput("maxRain", "Max rain (mm)", min=0, max=NA, value=1000)
                            )
                          ),
                          hr(),
                          DT::dataTableOutput("gaugetable")
                 ),
                 tabPanel("About",
                          htmlOutput("about")
                 ),
                          
                 conditionalPanel("false", icon("crosshair"))
)

# Define server logic
server <- function(input, output, session) {

  ## Connect to the database ###################################
  db <- openDB(db)

  # Check connection to database - abort if not connected
  if (!is.na(db$status)) { showNotification(ui = "Connected to database.", type = "default")
  } else { 
    showNotification(ui = "Unable to connect to database.", type = "error")  
    stopApp()
  }
  
  ## Get metadata table, or create if it doesn't exist ###############################
  if ("metadata" %in% dbListTables(db$con)) { metadata <- dbReadTable(db$con,"metadata")
  } else { metadata <- setNames(data.frame(matrix(ncol = 3, nrow = 0)), c("data", "lastupdate", "notes")) }
  

  ## Obtain and process gauge point data #############################################
  # If there is already a recent gauge table (< 12 hours since download), just load from database instead of downloading again
  t <- metadata %>% filter(data == "gauges") %>% select("lastupdate") %>% as.double()
  if (("gauges" %in% dbListTables(db$con)) && (as.double(now()) - t  < updateFreq) ) {
    rain$gauges <- loadGaugeSites(db)
    showNotification(ui = "Gauges loaded", type = "message")
    
  } else {
    ## Update gauges sites from ECan  ################################################
    id <- showNotification(ui = "Updating gauge sites...", type = "message", duration = NULL)
    getGaugeSites(db,metadata)
    rain$gauges <- loadGaugeSites(db)
    
    ## Add probability to gauge data #################################################
    rain$gauges <- getRainProbability(rain$gauges,db)
    
    ## Load geospatial data and produce a table for mapping and display  #############
    spatial <- loadSpatial()
    spatial <- processSpatial(spatial,rain$gauges)
    rain$gauges <- spatial$gauges@data
    
    ## Write back to database
    dbWriteTable(db$con, "gauges", rain$gauges, overwrite = TRUE)
    
    removeNotification(id)
    showNotification(ui = "Gauges updated", type = "message")
  }
  row.names(rain$gauges) <- rain$gauges$sitenumber

  # Disconnect from the database
  dbDisconnect(db$con)

  ## Get a list of guages and add to the site selection dialog
  gaugeList <- as.character(rain$gauges$sitenumber)
  gaugeName <- rain$gauges$site
  names(gaugeList) <- paste0(gaugeName," (",gaugeList,")")
  x <- ""
  names(x) <- "Select a gauge"
  gaugeList <- c(x,gaugeList)
  #sel <- sample(gaugeList, 1)
  updateSelectInput(session, inputId = "site", "Gauge:", choices = gaugeList)
  
  ## Copy the gauge data to another layer, to allow a reset later ####################
  rain$gaugestoday <- rain$gauges

  # Copy the data to reactiveValues list so that when they are update, the map/ table responds
  rv <- reactiveValues(data = rain$gauges, 
                       dataAll = rain$gauges, 
                       cleantable = generateCleantable(rain$gauges))
  
  ## Cross-match the date selection on the map and data table tables
  cur_val <- ""
  observe({
    # This observer depends on selectedDate and updates selectedDateTable with any changes
    if (cur_val != as.character(input$selectedDate)){
      # Then we assume selectedDateTable hasn't yet been updated
      updateTextInput(session, "selectedDateTable", NULL, input$selectedDate)
      cur_val <<- input$selectedDate
    }
  })
  observe({
    # This observer depends on selectedDateTable and updates selectedDate with any changes
    if (cur_val != as.character(input$selectedDateTable)){
      # Then we assume selectedDate hasn't yet been updated
      updateTextInput(session, "selectedDate", NULL, input$selectedDateTable)
      cur_val <<- input$selectedDateTable
    }
  })

  # End of initialisation
  
  #############################################################################################################################
  ## Interactive Map ##########################################################################################################
  
  observeEvent(input$selectedDate, { 
    if (input$selectedDate == today()) {
      # Reset to the default values downloaded
      rv$data <- rain$gauges
      rv$dataAll <- rain$gauges
      rv$cleantable <- generateCleantable(rain$gauges)
    } else {
      id <- showNotification(ui = paste0("Extracting data: ",input$selectedDate), type = "message", duration = NULL)
      
      # Extract data from all sites 
      # [future development: save these tables in the database, then check if date has already been extracted.]
      rain$gauges_datequery <- queryDate(db,rain$gauges,getDate = input$selectedDate)
      removeNotification(id)

      # Check there's actually some data for this date
      if ((sum(is.na(rain$gauges_datequery$rain_today)) == nrow(rain$gauges_datequery)) |
          (sum(is.na(rain$gauges_datequery$total_rainfall)) == nrow(rain$gauges_datequery))) {
        showNotification(ui = paste0("No data for ",input$selectedDate), type = "warning")
      } else {
        # Assign data to reactive variable to have map update
        rv$data <- rain$gauges_datequery
        rv$dataAll <- rain$gauges_datequery
        rv$cleantable <- generateCleantable(rain$gauges_datequery)
        
      }
    }
  })

  ## Create the map
  output$map <- renderLeaflet({
    leaflet(options = leafletOptions(worldCopyJump = TRUE, 
            crs = leafletCRS(proj4def = "+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0"))) %>%
      addTiles(
        urlTemplate = "http://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Dark_Gray_Base/MapServer/tile/{z}/{y}/{x}",
        attribution = 'Tiles &copy; Esri &mdash; Source: USGS, Esri, TANA, DeLorme, and NPS',
        options = tileOptions(opacity = 1, maxZoom = 9)
      ) %>%
      addTiles(
        urlTemplate = "http://server.arcgisonline.com/ArcGIS/rest/services/World_Terrain_Base/MapServer/tile/{z}/{y}/{x}",
        attribution = 'Tiles &copy; Esri &mdash; Source: USGS, Esri, TANA, DeLorme, and NPS',
        options = tileOptions(opacity = 0.3, maxZoom = 9)
      ) %>%
      addGraticule() %>%
      setView(lng = 172.6362, lat = -43.5321, zoom = 8) # Centre on Christchurch NZ
  })
  
  ## Returns the set of gauges that are in the bounds
  gaugeInBounds <- reactive({
    if (is.null(input$map_bounds))
      return(rv$data[FALSE,])
    bounds <- input$map_bounds
    latRng <- range(bounds$north, bounds$south)
    lngRng <- range(bounds$east, bounds$west)
    rv$data <- subset(rv$dataAll,
             latitude >= latRng[1] & latitude <= latRng[2] &
             longitude >= lngRng[1] & longitude <= lngRng[2])
    return(rv$data)
  })
  
  ## Histogram of selected variable
    output$histRain <- renderPlot({
      # If no gauges are in view, don't plot
      if (nrow(gaugeInBounds()) == 0)
        return(NULL)
      
      plotdata <- input$type
      if (plotdata == "rain_today") {
        main <- "24-hour rainfall"
        xlab <- "Rainfall (mm)"
      } else if (plotdata == "total_rainfall") {
        main <- "7-day rainfall"
        xlab <- "Rainfall (mm)"
      }
  
      #histdata <- gaugeInBounds()[[plotdata]]
      histdata <- gaugeInBounds()
  
      ggplot(data=histdata, aes(histdata[[plotdata]])) + 
        geom_histogram(color='darkblue',fill = 'blue',bins = 20) +
        theme(axis.text = element_text(color = "black")) +
        labs(x = "Rain (mm)", y = "Number of gauges")
  })

  ## Rainfall bargraph timeseries plot
  output$timeSeries <- renderPlot({
    if (!is.null(input$site) & (input$site != "")) {
       data <- loadSiteData(input$site,db)
      
      ggplot(data, aes(x=data$datetime, y=data$rainfalltotal)) + 
        geom_bar(stat="identity",color='darkblue',fill = 'blue') +
        labs(x = "Date", y = "Rainfall (mm)")
    } 
  })

  ## IDF curves plot
  output$IDFplot <- renderPlot({
    if (!is.null(input$site) & (input$site != "")) {
      data <- loadIDF(input$site,db)
      data_long <- melt(data, id="Duration")  # convert to long format
      ggplot(data=data_long,aes(x=Duration, y=value, colour=variable)) +
        geom_line() +
        labs(colour = "Return period", x = "Duration (days)", y = "Intensity (mm/day)") +
        theme(legend.position="bottom", legend.box = "horizontal") +
        scale_y_continuous(trans='log2') + scale_x_continuous(trans='log2')
    } 
  })
  
  ## Add circle markers for each gauge to the map:
  #  Size and colour depends on the magnitude and likelihood of the rainfall values
  observe({
    colorBy <- paste0(input$type,"_prob")
    sizeBy <- input$type
    colorData <- 1/rv$data[[colorBy]] # convert from annual exceedance probability to return period
    
    # Create a stretched rainbow colormap to allow both low and high values to be differentiated
    cmap <- c(rainbow(10, start = 0.25, end = 0.7), rainbow(30,start = 0.7, end = 1), rainbow(60, start = 0,end = 0.18))
      pal <- colorNumeric(cmap, c(0,400), na.color = "#606060", reverse = FALSE)

    #r <- rv$data[[sizeBy]]
    maxr <- 200 
    radius <- (rv$data[[sizeBy]] / maxr * 10000) + 2000 # using 1000 m as a minumum radius (i.e. for 0 rainfall)

    leafletProxy("map", data = rv$data) %>%
      clearShapes() %>%
      addCircles(~longitude, ~latitude, radius=radius, layerId=~sitenumber,
                 stroke=FALSE, fillOpacity=0.8, fillColor=pal(colorData)) %>%
      addLegend("bottomleft", pal=pal, values=colorData, title="Return period",
                layerId="colorLegend")
  })
  

  ## Show a popup at the given location
  showGaugePopup <- function(sitenumber, lat, lng) {
    selectedGauge <- rv$data[rv$data$sitenumber == sitenumber,] #allzips[allzips$zipcode == zipcode,]
    content <- as.character(tagList(
      tags$h5(selectedGauge$site),
      tags$strong(HTML(sprintf("Site number: %s",selectedGauge$sitenumber))), 
      tags$br(),
      sprintf("Date: %s", input$selectedDate), 
      tags$br(),
      sprintf("Rain previous 24 hours: %s mm", selectedGauge$rain_today), 
      tags$br(),
      sprintf("Rain previous 7 days: %s mm", selectedGauge$total_rainfall)
    ))
    leafletProxy("map") %>% addPopups(lng, lat, content, layerId = sitenumber)
  }
  
  ## When map is clicked, show a popup with gauge info
  observe({
    leafletProxy("map") %>% clearPopups()
    event <- input$map_shape_click
    if (is.null(event))
      return()
    isolate({
      showGaugePopup(event$id, event$lat, event$lng)
      #print(event$id)
      updateSelectInput(session, inputId = "site", selected = event$id)
      
    })
  })
  
  #############################################################################################################################
  ## Data Explorer ############################################################################################################
  
  observe({
    district <- if (is.null(input$region)) character(0) else {
      filter(rv$cleantable, Region %in% input$region) %>%
        `$`('District') %>%
        unique() %>%
        sort()
    }
    stillSelected <- isolate(input$district[input$district %in% district])
    updateSelectInput(session, "district", choices = district,
                      selected = stillSelected)
  })
  
  observe({
    sitecodes <- if (is.null(input$region)) character(0) else {
      rv$cleantable %>%
        filter(Region %in% input$region,
               is.null(input$district) | District %in% input$district) %>%
        `$`('Gauge') %>%
        unique() %>%
        sort()
    }
    stillSelected <- isolate(input$sitecodes[input$sitecodes %in% sitecodes])
    updateSelectInput(session, "sitecodes", choices = sitecodes,
                      selected = stillSelected)
  })
  
  observe({
    if (is.null(input$goto))
      return()
    isolate({
      map <- leafletProxy("map")
      map %>% clearPopups()
      dist <- 0.5
      zip <- input$goto$zip
      lat <- input$goto$lat
      lng <- input$goto$lng
      showGaugePopup(zip, lat, lng)
      updateSelectInput(session, inputId = "site", selected = zip)
      map %>% fitBounds(lng - dist, lat - dist, lng + dist, lat + dist)
    })
  })
  
  output$gaugetable <- DT::renderDataTable({
    ## Update the input sections on the data table based on the loaded data
    regions <- rv$cleantable %>% distinct(Region) %>% pull()
    stillSelected <- isolate(input$region[input$region %in% regions])
    updateSelectInput(session, "region", choices = regions,selected = stillSelected)

    df <- rv$cleantable %>%
      filter(
        RainDay >= input$minRain,
        RainDay <= input$maxRain,
        is.null(input$region) | Region %in% input$region,
        is.null(input$district) | District %in% input$district,
        is.null(input$sitecodes) | Gauge %in% input$sitecodes
      ) %>%
      mutate(Action = paste('<a class="go-map" href="" data-lat="', Lat, '" data-long="', Lon, '" data-zip="', Gauge, '"><i class="fa fa-crosshairs"></i></a>', sep=""))
    action <- DT::dataTableAjax(session, df)
    
    DT::datatable(df, options = list(ajax = list(url = action)), escape = FALSE)
  })
  
  #############################################################################################################################
  ## About ####################################################################################################################
  
  ## Add text to the About tab
  output$about <- renderText({ aboutText })
  
  
  
  
  ######## TO DO - UPDATE IN BACKGROUND:
  # update site data in background after page load
  #
  #updateData(db)  # Download the latest date for all sites
  #gaugesStats(db) # Process statistics for each site/ get IDF etc
  
}
  

# Run the application 
shinyApp(ui = ui, server = server)

