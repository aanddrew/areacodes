import matplotlib.pyplot as plt
import geopandas

# https://www.census.gov/geographies/mapping-files/time-series/geo/carto-boundary-file.html
states = geopandas.read_file('cb_2018_us_state_20m.shp')
states = states.set_crs('epsg:4269')
states = states.to_crs('epsg:3395')#"EPSG:3395")

ax = states.boundary.plot(linewidth=0.5)

h = 0.5 * 1E7
w = h * (4 / 3)
x = -1.4 * 1E7
y = 2.6 * 1E6
ax.set_xlim(x, x + w)
ax.set_ylim(y, y + h)
# ax.axis('equal')
ax.set_axis_off()
plt.savefig('map_states_borders.png', dpi=1000)
# plt.show()