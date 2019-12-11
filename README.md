# BD210 Data Engineering Final December 2019
 Final project for UW BD210
 
 
## UW BD210 Data Engineering Final
-------
#### Wholesale Electricity LBMP & Irradiance Adjusted PV Revenue
-------
I began the project downloading hourly locational based marginal price (LBMP) data from the PJM website, which is the price seen at power plants, and other interfaces with the grid across their service territory in the Mid/South-Atlantic and Midwest.

The price is determined in a day-ahead auction by PJM, the market operator. The highest price bid accepted into the auction for that hour sets price paid to all generators at that hour (generally gas generators setting price), when auction clears all generation has been procured as cheaply as possible by PJM to satisfy load. The transmission constraints and other environmental factors are reflected as add-ons to price resulting in what we see in this data as LBMP. Certain regions have higher congestion on lines due to population, industry etc.. and can exhibit higher additional costs to deliver energy there. We can see in the data, high prices cluster around population centers. In 2018, for reduced dataset I chose there were 4688 unique pnodes (pricing nodes) with complete annual data, or 41,066,880 entries and a ~3GB csv file.


#### US Energy Markets
![test image size](/img/powermarkets.jpg)

### Links to Data and Notebook

[PJM's LMP Model Info](https://www.pjm.com/markets-and-operations/energy/lmp-model-info.aspx) | [PJM Tools Data Access](https://www.pjm.com/markets-and-operations/etools.aspx)


____


#### Further data considered

___Joining by location and date to determine where and when solar facilities might best capture revenue from unique market areas and their price swings___ 

-----

**Solar irradiance data from NREL**

[NREL NSRDB Data Viewer](https://maps.nrel.gov/nsrdb-viewer/)

**Reference case of hourly modeled production at a solar pv facility for one year**

N/A

----

#### Goals

___

* Determine annual price averages at pricing nodes
* Determine annual revenues captured by a solar facility
* Determine highest value nodes and regions for solar
* Present findings in Map
* Complete analysis for multiple years (incomplete)


___

#### 2018 LBMP Averages ($/MWh)
![test image size](/img/pjm_2018_lbmp.png)

#### 2018 GHI Adjusted PV Revenue ($/MWh)
![test image size](/img/pjm_2018_revenue.png)
