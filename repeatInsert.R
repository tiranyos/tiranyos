
# Load Necessary Packages

library(dplyr)
library(tidyr)

############################################################################################################################################################

chunkByLimit <- function (dataFrame,limit,column, groupBy) {

    excludedCols <- colNames[colNames != column]            # Find every other columnName which is not used in df

    dataFrameLimit <- dataFrame %>%
        select(-one_of(excludedCols)) %>%
     
        mutate(Remainder=(!!sym(column)) %% limit,           # get remainders
               freq=((!!sym(column)) %/% limit) + 1) %>%     # get freqs +1 (that row will be changed with the remaining later)
        
        mutate(!! column := limit) %>%                       # change Column to threshold
        uncount(freq)  %>%                                   # uncount Freqs
        group_by((!!sym(groupBy))) %>%                       # group by Given Col
        mutate(!! column :=  replace((!!sym(column)),
                                                 n(), 
                                     last(Remainder)         # change last value of group to remainder value
                                    )
              ) %>%
       filter((!!sym(column)) != 0)  %>%                     # filter out 0 values
       select(-Remainder)                                # unselect remainderColumn
       

    return(dataFrameLimit) 
}
############################################################################################################################################################

fusionHa <- function(dataFrame,limit,colNames, groupBy) {

dataFrameList <- list()

# Create a list of dataFrames
for (column in colNames) {
dataFrameList[[column]] <- chunkByLimit(dataFrame,limit,column, groupBy) %>% # Call chunkByLimit for every df in list
mutate(row = row_number())                                                   # Add row Number to reduce duplicates
print(dataFrameList)
    }

# Merge dataFrames in List
BindedResults <- Reduce(full_join,dataFrameList) %>%
                 select(-row)

# Replace NAs with 0s
BindedResults[is.na(BindedResults)] = 0 
result <<- BindedResults %>%
            arrange(!!sym(groupBy))                                         # Arrange by groupBy col


}

############################################################################################################################################################

# Pass DataFrame

dataFrame <-  data.frame(
                 Name  = c("Krillin", "Goku", "Gohan"),
                 PowerLevel = c(42000, 0, 1100),
                 WillPower = c(0, 20000, 10500),
                 Strength = c(42000,7500,52000)
)

# Maximum chunk
limit <- 10000

# vector of colNames to Chunk 

colNames <- c("PowerLevel","WillPower", "Strength") # Cols To Chunk
groupBy <- "Name"                          # Group By column

############################################################################################################################################################

fusionHa(dataFrame,limit,colNames, groupBy)

# Print out DataFrame
print(dataFrame)
# Print out Results
print(result)
