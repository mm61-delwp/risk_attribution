# Attribution of risk within the new Bushfire Risk Modelling Framework

The new bushfire risk modelling framework allocates bushfire risk to modelled bushfire ignition locations. This is because it uses a Bayesian network to incorporate elements of likelihood including ignition, weather and suppression, and the inputs to this Bayes net are ignition-based.

This poses some challenges for communicating risk. For example, people within a locality, shire or district expect the risk data we provide them to reflect bushfire risk _to_ assets in their locality, shire or district, not the risk _from_ ignitions within their footprint pose assets which may be outside their area of interest.

There has been some debate about whether it is appropriate to 

This may not be true. It's easy to imagine a scenario where a modelled fire impacts on assets before the Bayes net is included, 

An alternative way of looking at the scenario is that the Phoenix fire footprint and losses represent a worst case scenario for the modelled weather and ignition, and the Bayes net is downscaling the _likelihood_ of the worst case occurring. 

Demonstration of a process for new allocating risk outputs from new Bushfire Risk Modelling Framework to:
* the ignitions that cause the risk of house loss (ignition risk) - not shown here as it is the standard output 
* the fire paths that contribute to risk of loss (spread risk)
* the locations where the risk of loss occurs (impact risk)
* treatable areas within fire paths for prioritising fuel management over long (e.g. fire management zoning/strategies) or short (e.g. JFMP) timeframes. 

### 1. Spread Risk - allocate house loss risk to fire paths

**Example query to calculate the contribution of each 180m grid cell to phoenix and bayes net house losses:**


Requires:
* Bayes net output - replace bn_jfmp_2025_no_jfmp_2cd658671c434fcaa7254f0cfe3fc99 with job id
* Rank 1 to 10 weather scenarios - replace jfmp_2023_2022fh_2km_nojfmp_v2_70deb0b6f5524cb98570dfdc875189eb with group id

> Note: This query takes time to run due to the amount of data it reads - The below example = Run time: 8 min 17.608 sec    Data scanned: 781.37 GB
```sql
with
bayesnet as (select * from bn_jfmp_2025_no_jfmp_2cd658671c434fcaa7254f0cfe3fc99d.bn_ignition_summary),
allcells_wx01 as (select *, 'wx01' as weather from jfmp_2023_2022fh_2km_nojfmp_v2_70deb0b6f5524cb98570dfdc875189eb1.cell where intensity > 0 or emberdensity > 0.5),
allcells_wx02 as (select *, 'wx02' as weather from jfmp_2023_2022fh_2km_nojfmp_v2_70deb0b6f5524cb98570dfdc875189eb2.cell where intensity > 0 or emberdensity > 0.5),
allcells_wx03 as (select *, 'wx03' as weather from jfmp_2023_2022fh_2km_nojfmp_v2_70deb0b6f5524cb98570dfdc875189eb3.cell where intensity > 0 or emberdensity > 0.5),
allcells_wx04 as (select *, 'wx04' as weather from jfmp_2023_2022fh_2km_nojfmp_v2_70deb0b6f5524cb98570dfdc875189eb4.cell where intensity > 0 or emberdensity > 0.5),
allcells_wx05 as (select *, 'wx05' as weather from jfmp_2023_2022fh_2km_nojfmp_v2_70deb0b6f5524cb98570dfdc875189eb5.cell where intensity > 0 or emberdensity > 0.5),
allcells_wx06 as (select *, 'wx06' as weather from jfmp_2023_2022fh_2km_nojfmp_v2_70deb0b6f5524cb98570dfdc875189eb6.cell where intensity > 0 or emberdensity > 0.5),
allcells_wx07 as (select *, 'wx07' as weather from jfmp_2023_2022fh_2km_nojfmp_v2_70deb0b6f5524cb98570dfdc875189eb7.cell where intensity > 0 or emberdensity > 0.5),
allcells_wx08 as (select *, 'wx08' as weather from jfmp_2023_2022fh_2km_nojfmp_v2_70deb0b6f5524cb98570dfdc875189eb8.cell where intensity > 0 or emberdensity > 0.5),
allcells_wx09 as (select *, 'wx09' as weather from jfmp_2023_2022fh_2km_nojfmp_v2_70deb0b6f5524cb98570dfdc875189eb9.cell where intensity > 0 or emberdensity > 0.5),
allcells_wx10 as (select *, 'wx10' as weather from jfmp_2023_2022fh_2km_nojfmp_v2_70deb0b6f5524cb98570dfdc875189eb10.cell where intensity > 0 or emberdensity > 0.5),
hl_cell as (
    select *,       'wx01' as weather from jfmp_2023_2022fh_2km_nojfmp_v2_70deb0b6f5524cb98570dfdc875189eb1.hl_cell where sum_hl_int > 0
    union select *, 'wx02' as weather from jfmp_2023_2022fh_2km_nojfmp_v2_70deb0b6f5524cb98570dfdc875189eb2.hl_cell where sum_hl_int > 0
    union select *, 'wx03' as weather from jfmp_2023_2022fh_2km_nojfmp_v2_70deb0b6f5524cb98570dfdc875189eb3.hl_cell where sum_hl_int > 0
    union select *, 'wx04' as weather from jfmp_2023_2022fh_2km_nojfmp_v2_70deb0b6f5524cb98570dfdc875189eb4.hl_cell where sum_hl_int > 0
    union select *, 'wx05' as weather from jfmp_2023_2022fh_2km_nojfmp_v2_70deb0b6f5524cb98570dfdc875189eb5.hl_cell where sum_hl_int > 0
    union select *, 'wx06' as weather from jfmp_2023_2022fh_2km_nojfmp_v2_70deb0b6f5524cb98570dfdc875189eb6.hl_cell where sum_hl_int > 0
    union select *, 'wx07' as weather from jfmp_2023_2022fh_2km_nojfmp_v2_70deb0b6f5524cb98570dfdc875189eb7.hl_cell where sum_hl_int > 0
    union select *, 'wx08' as weather from jfmp_2023_2022fh_2km_nojfmp_v2_70deb0b6f5524cb98570dfdc875189eb8.hl_cell where sum_hl_int > 0
    union select *, 'wx09' as weather from jfmp_2023_2022fh_2km_nojfmp_v2_70deb0b6f5524cb98570dfdc875189eb9.hl_cell where sum_hl_int > 0
    union select *, 'wx10' as weather from jfmp_2023_2022fh_2km_nojfmp_v2_70deb0b6f5524cb98570dfdc875189eb10.hl_cell where sum_hl_int > 0
    ),

-- select house loss field from bayes net table (!!Note: if houseloss_mean_res is not the best measure to use, replace it!!)
bn_losses as (
    select
        ignition_id as ignitionid,
        houseloss_mean_res as ignition_houseloss_bn
    from
        bayesnet
    ),

-- sum phoenix losses (house loss model) to weather and ignition 
phx_losses as (
    select
        weather,
        ignitionid,
        sum(sum_hl_int) as ignition_houseloss_phx
    from
        hl_cell
    group by
        weather,
        ignitionid
    ),

-- create ignition table with a count of 'fire area' (not burnt area) cells
ignition_num_cells as (
    select weather, ignitionid, count(cellid) as fire_area_cells from allcells_wx01 group by weather, ignitionid
    union select weather, ignitionid, count(cellid) as fire_area_cells from allcells_wx02 group by weather, ignitionid
    union select weather, ignitionid, count(cellid) as fire_area_cells from allcells_wx03 group by weather, ignitionid
    union select weather, ignitionid, count(cellid) as fire_area_cells from allcells_wx04 group by weather, ignitionid
    union select weather, ignitionid, count(cellid) as fire_area_cells from allcells_wx05 group by weather, ignitionid
    union select weather, ignitionid, count(cellid) as fire_area_cells from allcells_wx06 group by weather, ignitionid
    union select weather, ignitionid, count(cellid) as fire_area_cells from allcells_wx07 group by weather, ignitionid
    union select weather, ignitionid, count(cellid) as fire_area_cells from allcells_wx08 group by weather, ignitionid
    union select weather, ignitionid, count(cellid) as fire_area_cells from allcells_wx09 group by weather, ignitionid
    union select weather, ignitionid, count(cellid) as fire_area_cells from allcells_wx10 group by weather, ignitionid
    ),

-- join the Bayes net and Phoenix house losses to the ignition summary table and divide phoenix and bayes net losses among fire area cells 
-- note that Phoenix losses are per weather/ignition combination and Bayes net losses are per ignition (the BN combines weathers)
-- so the joined Bayes net loss value is joined to every weather/ignition combo then averaged to remove duplication 
ignition_summary as(
    select 
        i.ignitionid,
        i.weather,
        sum(i.fire_area_cells) as fire_area_cells,
        ph.ignition_houseloss_phx as ignition_houseloss_phx,
        avg(bn.ignition_houseloss_bn) as ignition_houseloss_bn,
        cast(avg(bn.ignition_houseloss_bn) as real)/cast(ph.ignition_houseloss_phx as real) as weight_bn,
        cast(ph.ignition_houseloss_phx as real)/cast(sum(i.fire_area_cells) as real) as cell_phx_contribution,
        cast(avg(bn.ignition_houseloss_bn) as real)/cast(sum(i.fire_area_cells) as real) as cell_bn_contribution
    from 
        ignition_num_cells i
        left join phx_losses ph on ph.ignitionid = i.ignitionid and ph.weather = i.weather
        left join bn_losses bn on bn.ignitionid = i.ignitionid
    group by
        i.ignitionid,
        i.weather,
        ph.ignition_houseloss_phx
    ),

-- join the cell contribution to phoenix and bayes net losses back to the allcells tables
allcells as(
    select ph.cellid, ph.ignitionid, ph.weather, sum(i.cell_bn_contribution) as bn_contribution, sum(i.cell_phx_contribution) as phx_contribution
    from allcells_wx01 ph inner join ignition_summary i on i.ignitionid = ph.ignitionid and i.weather = ph.weather
    group by ph.cellid, ph.ignitionid, ph.weather
    union
    select ph.cellid, ph.ignitionid, ph.weather, sum(i.cell_bn_contribution) as bn_contribution, sum(i.cell_phx_contribution) as phx_contribution
    from allcells_wx02 ph inner join ignition_summary i on i.ignitionid = ph.ignitionid and i.weather = ph.weather
    group by ph.cellid, ph.ignitionid, ph.weather
    union
    select ph.cellid, ph.ignitionid, ph.weather, sum(i.cell_bn_contribution) as bn_contribution, sum(i.cell_phx_contribution) as phx_contribution
    from allcells_wx03 ph inner join ignition_summary i on i.ignitionid = ph.ignitionid and i.weather = ph.weather
    group by ph.cellid, ph.ignitionid, ph.weather
    union
    select ph.cellid, ph.ignitionid, ph.weather, sum(i.cell_bn_contribution) as bn_contribution, sum(i.cell_phx_contribution) as phx_contribution
    from allcells_wx04 ph inner join ignition_summary i on i.ignitionid = ph.ignitionid and i.weather = ph.weather
    group by ph.cellid, ph.ignitionid, ph.weather
    union
    select ph.cellid, ph.ignitionid, ph.weather, sum(i.cell_bn_contribution) as bn_contribution, sum(i.cell_phx_contribution) as phx_contribution
    from allcells_wx05 ph inner join ignition_summary i on i.ignitionid = ph.ignitionid and i.weather = ph.weather
    group by ph.cellid, ph.ignitionid, ph.weather
    union
    select ph.cellid, ph.ignitionid, ph.weather, sum(i.cell_bn_contribution) as bn_contribution, sum(i.cell_phx_contribution) as phx_contribution
    from allcells_wx06 ph inner join ignition_summary i on i.ignitionid = ph.ignitionid and i.weather = ph.weather
    group by ph.cellid, ph.ignitionid, ph.weather
    union
    select ph.cellid, ph.ignitionid, ph.weather, sum(i.cell_bn_contribution) as bn_contribution, sum(i.cell_phx_contribution) as phx_contribution
    from allcells_wx07 ph inner join ignition_summary i on i.ignitionid = ph.ignitionid and i.weather = ph.weather
    group by ph.cellid, ph.ignitionid, ph.weather
    union
    select ph.cellid, ph.ignitionid, ph.weather, sum(i.cell_bn_contribution) as bn_contribution, sum(i.cell_phx_contribution) as phx_contribution
    from allcells_wx08 ph inner join ignition_summary i on i.ignitionid = ph.ignitionid and i.weather = ph.weather
    group by ph.cellid, ph.ignitionid, ph.weather
    union
    select ph.cellid, ph.ignitionid, ph.weather, sum(i.cell_bn_contribution) as bn_contribution, sum(i.cell_phx_contribution) as phx_contribution
    from allcells_wx09 ph inner join ignition_summary i on i.ignitionid = ph.ignitionid and i.weather = ph.weather
    group by ph.cellid, ph.ignitionid, ph.weather
    union
    select ph.cellid, ph.ignitionid, ph.weather, sum(i.cell_bn_contribution) as bn_contribution, sum(i.cell_phx_contribution) as phx_contribution
    from allcells_wx10 ph inner join ignition_summary i on i.ignitionid = ph.ignitionid and i.weather = ph.weather
    group by ph.cellid, ph.ignitionid, ph.weather
    )
    
-- summarise to cellid by summing each cell's contribution to bayes net and phoenix losses for all ignitions and weather scenarios
select 
    cell.cellid, cell.x_coord, cell.y_coord,
    sum(coalesce(a.phx_contribution, 0)) as phx_contribution,
    sum(coalesce(a.bn_contribution, 0)) as bn_contribution
from
    reference_brau.grid_cell_180m cell left join allcells a on a.cellid = cell.cellid
where
    state = 'Victoria'
group by
    cell.cellid, cell.x_coord, cell.y_coord
```
**Map of cell contribution to Phoenix house losses (log scale symbology):**
![image](https://user-images.githubusercontent.com/100050237/231656481-fe64eea4-7771-49cc-a1ce-0339a420ffdb.png)

**Map of cell contribution to Bayes net house losses (log scale symbology):**
![image](https://user-images.githubusercontent.com/100050237/231657140-6e3b6215-d368-428f-9d85-cf02297cb493.png)

### 2. Treatable Spread Risk - treatable contribution to house losses

The spread risk method demonstrated above tells us about the contribution of each part of the state to house loss risk.
It can't be used for prioritising fuel treatment by planned burning, as it does not consider whether the risk can be mitigated by planned burning.

* drop grid cells where treatability = 0
* sum phx_contribution and bn_contribution to burn units (or other geographic units as required)

[INSERT DEMONSTRATION]

### 3. Impact Risk - allocate risk to location of modelled house loss
