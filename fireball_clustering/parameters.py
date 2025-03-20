'''
Parameter values for the data analysis pipeline.

Author: Armaan Mahajan
'''
# Intensity cutoff for what is considered a fireball when multiplied by datasets std
CUTOFF = 3

# Window sizes for moving average and moving standard dev.
AVG_WINDOW = 30
STD_WINDOW = 30

# Temporal Proximity to FR events in seconds
FR_EVENT_PROXIMITY = 10

# Min number of stations we must have data for 
# to begin analysis (within 1000km)
MIN_CAMERAS = 1/3
