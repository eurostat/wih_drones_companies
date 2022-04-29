library(tidyverse)
library(data.table)
library(sf)
library(tmap)
library(tmaptools)
options(viewer=NULL)

get_data = function(file, country) {
	x = data.table::fread(file)[region!=""]
	
	cty = x$region
	cty2 = strsplit(cty, ", ", fixed = TRUE)
	w2 = lapply(cty2, function(ct) rep(1, length(ct))) # 1/length(ct)
	
	df = data.frame(cty = unlist(cty2), w = unlist(w2)) %>% 
		filter(!is.na(cty)) %>% 
		group_by(cty) %>% 
		summarize(count = sum(w)) %>% 
		ungroup()
	
	df$point = st_geometry(tmaptools::geocode_OSM(paste0(df$cty, ", ", country), as.sf = TRUE))
	df = st_as_sf(df)
	saveRDS(df, file = paste0("df_", country, ".rds"))
	df
}

dfs = mapply(get_data, file = c("4_Result_ES_3_selected_info.csv", "4_Result_IT_3_selected_info.csv", "4_Result_IE_3_selected_info.csv"),
			 country = c("Spain", "Italy", "Ireland"), SIMPLIFY = FALSE)


############################################3
# Spain
############################################3




df = dfs[[1]]


df$ymod = -0.5 - (df$count / 93) * 0.9

tmap_mode("view")

tm_shape(df) +
	tm_basemap("CartoDB.PositronNoLabels") +
	tm_bubbles(size = "count", col = "#F0DE82") + # 	#E69F00
	tm_shape(df) +
	tm_bubbles(size = "count", shape = tmap_icons("drone3_orange.png"), scale = 0.7) +
	tm_text("count", ymod = df$ymod) +
	tm_tiles("CartoDB.PositronOnlyLabels") +
	tm_view(symbol.size.fixed = TRUE)


# Canarische eilanden
tm_shape(df) +
	tm_basemap("CartoDB.PositronNoLabels") +
	tm_bubbles(size = "count", col = "#F0DE82", scale = 2) + # 	#E69F00
	tm_shape(df) +
	tm_bubbles(size = "count", shape = tmap_icons("drone3_orange.png"), scale = 1.2) +
	tm_text("count2", ymod = df$ymod) +
	tm_tiles("CartoDB.PositronOnlyLabels") +
	tm_view(symbol.size.fixed = TRUE)


############################################3
# Italy
############################################3

df = dfs[[2]]


df$ymod = -0.5 - (df$count / 33) * 0.9

tmap_mode("view")

tm_shape(df) +
	tm_basemap("CartoDB.PositronNoLabels") +
	tm_bubbles(size = "count", col = "#56B4E9") + # 	#E69F00
	tm_shape(df) +
	tm_bubbles(size = "count", shape = tmap_icons("drone3_white.png"), scale = 0.7) +
	tm_text("count", ymod = df$ymod) +
	tm_tiles("CartoDB.PositronOnlyLabels") +
	tm_view(symbol.size.fixed = TRUE)


############################################3
# Ireland
############################################3


nuts3 = st_read("NUTS3_Boundaries_Generalised_100m_-_OSi_National_Statistical_Boundaries_-_2015.geojson")
nuts3b = rmapshaper::ms_simplify(nuts3, keep = 0.3)



df = dfs[[3]]

df$cty[df$cty=="West region"] = "West Region"

df$ymod = -0.5 - (df$count / 15) * 0.9

df$ymod[df$cty == "Mid-East Region"] = -3.5
df$ymod[df$cty == "Border Region"] = -5

df$xmod = 0
df$xmod[df$cty == "Border Region"] = -2.5

df$ymodB = 0
df$ymodB[df$cty == "Mid-East Region"] = -2.5
df$ymodB[df$cty == "Border Region"] = -4

df$xmodB = 0
df$xmodB[df$cty == "Border Region"] = -2.5


df$ymodT = c(`Border Region` = -6, `Dublin Region` = 3, `Mid-East Region` = -4.5, 
			`Mid-West Region` = -2.5, `Midlands Region` = -1.5, `South-East Region` = -2, 
			`South-West Region` = 2, `West Region` = -2)

df$xmodT = c(`Border Region` = -2, `Dublin Region` = 10, `Mid-East Region` = 3.5, 
			 `Mid-West Region` = 0, `Midlands Region` = 0, `South-East Region` = -2, 
			 `South-West Region` = 0, `West Region` = 0)


tmap_mode("view")

tm_shape(nuts3b) + 
	tm_borders(col = "grey70", lwd = 1) +
	#tm_text("NUTS3NAME") +
tm_shape(df) +
	tm_basemap("CartoDB.PositronNoLabels") +
	tm_bubbles(size = "count", col = "#87C55F", scale = 1.3, ymod = df$ymodB, xmod = df$xmodB) + # 	#E69F00
	tm_shape(df) +
	tm_bubbles(size = "count", shape = tmap_icons("drone3_white.png"), scale = 0.7, ymod = df$ymodB, xmod = df$xmodB) +
	tm_text("count", ymod = df$ymod, xmod = df$xmod) +
	tm_shape(df) +
	tm_text("cty", ymod = df$ymodT, xmod = df$xmodT) +
	tm_tiles("CartoDB.PositronOnlyLabels") +
	tm_view(symbol.size.fixed = TRUE)
