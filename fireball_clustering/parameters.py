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
