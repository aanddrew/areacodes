import matplotlib.pyplot as plt
import geopandas

# https://www.census.gov/geographies/mapping-files/time-series/geo/carto-boundary-file.html
states = geopandas.read_file('cb_2018_us_state_20m.shp')
# states = states.to_crs({'init': 'epsg:3395'})#"EPSG:3395")

ax = states.boundary.plot(linewidth=0.25)

ax.set_xlim(-125, -65)
ax.set_ylim(23, 50)
ax.set_axis_off()
plt.savefig('map_states_borders.png', dpi=1000)
# plt.show()