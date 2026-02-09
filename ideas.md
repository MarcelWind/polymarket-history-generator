###########
## Ideas ## 
###########

- Analyze average volume per weather location overall and to each point x in time. (for future comparisons intraday)
    - Listener for sudden volume spikes on individual markets of an event (e.g. 3x average volume in last hour) and alert user

- Analyze locations over multiple event dates and find patterns in the time windows when volume surges or clusters. 
    - Then listen which markets are primarily bet on and which side aggressive market orders are placed (YES/NO). Tracking CUMULATIVE DELTA Orderflow.

- Volume profile beats TPO. Less relevant where market spent most time, more relevant WHEN and WHERE big volume is being bet (=potential smart money) - in relation to the other markets.

- Process market price in relation to its VWAP, after volume threshold and/or most active time period before maturity has been met.
    - Bias is to NO the imbalanced markets down and YES the imbalanced market up (if there is one clear winner remaining). 
    - Perhaps fade the mean-reverting markets earlier in the less active times before volume surges – if there is enough time opportunity and reward/risk. Focusing on the lesser likely to win but overvalued NO markets.

- Place NO trades early and first after market discovery based on statistically safe distance to forecast data. Only place YES trade after volume analysis shows clear winner. Optionally add on NO positions then as well. 

- Also possible to bet YES on more than 1 strong imbalance up market, but use stop loss and take profit orders on overvalued readings