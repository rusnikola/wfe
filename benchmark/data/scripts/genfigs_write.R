# Based on IBR's genfigs.R.
# Copyright 2015 University of Rochester
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License. 

###############################################################
### This script generates the 8 plots that were actually    ###
### used in the paper using the data contained in ../final/ ###
###############################################################

library(plyr)
library(ggplot2)

filenames<-c("hashmap","list","natarajan","wfqueue","crturnqueue")
for (f in filenames){
read.csv(paste("../final/",f,"_result.csv",sep=""))->lindata

lindata$environment<-as.factor(gsub("tracker=RCU","EBR",lindata$environment))
lindata$environment<-as.factor(gsub("tracker=HE","HE",lindata$environment))
lindata$environment<-as.factor(gsub("tracker=Hazard","HP",lindata$environment))
lindata$environment<-as.factor(gsub("tracker=NIL","Leak Memory",lindata$environment))
lindata$environment<-as.factor(gsub("tracker=WFE","WFE",lindata$environment))
lindata$environment<-as.factor(gsub("tracker=Range_new","2GEIBR",lindata$environment))

# Compute average and max retired objects per operation from raw data
ddply(.data=lindata,.(environment,threads),mutate,retired_avg= mean(obj_retired)/(mean(ops)))->lindata
ddply(.data=lindata,.(environment,threads),mutate,ops_max= max(ops)/(interval*1000000))->lindata

nildatalin <- subset(lindata,environment=="Leak Memory")
rcudatalin <- subset(lindata,environment=="EBR")
hazarddatalin <- subset(lindata,environment=="HP")
hedatalin <- subset(lindata,environment=="HE")

fedatalin <- subset(lindata,environment=="WFE")
rangenewdatalin <- subset(lindata,environment=="2GEIBR")

lindata = rbind(fedatalin, hedatalin, rangenewdatalin, rcudatalin, hazarddatalin, nildatalin)
lindata$environment <- factor(lindata$environment, levels=c("WFE", "EBR", "HE", "HP", "2GEIBR", "Leak Memory"))

# Set up colors and shapes (invariant for all plots)
color_key = c("#000000", "#0000FF", "#FFAA1D", "#013220",
              "#7F7F7F", "#FF0000")

names(color_key) <- unique(c(as.character(lindata$environment)))

shape_key = c(17,1,2,62,4,18)
names(shape_key) <- unique(c(as.character(lindata$environment)))

line_key = c(1,1,2,4,4,1,4)
names(line_key) <- unique(c(as.character(lindata$environment)))

#####################################
#### Begin charts for throughput ####
#####################################

legend_pos=c(0.4,0.92)
y_range_down = 0
y_range_up = 4

# Benchmark-specific plot formatting
if(f=="list"){
  y_range_down=0
  y_range_up=0.11
}else if(f=="natarajan"){
  y_range_up=55
}else if(f=="hashmap"){
  y_range_up=130
  legend_pos=c(0.33,0.92)
}else if(f=="wfqueue"){
  y_range_up=2.3
  legend_pos=c(0.56,0.92)
}

# Generate the plots
linchart<-ggplot(data=lindata,
                  aes(x=threads,y=ops_max,color=environment, shape=environment, linetype=environment))+
  geom_line()+xlab("Number of Threads")+ylab("Mops / second")+geom_point(size=5)+
  scale_shape_manual(values=shape_key[names(shape_key) %in% lindata$environment])+
  scale_linetype_manual(values=line_key[names(line_key) %in% lindata$environment])+
  theme_bw()+ guides(shape=guide_legend(title=NULL,nrow = 2))+ 
  guides(color=guide_legend(title=NULL,nrow = 2))+
  guides(linetype=guide_legend(title=NULL,nrow = 2))+
  scale_color_manual(values=color_key[names(color_key) %in% lindata$environment])+
  scale_x_continuous(breaks=c(1,8,16,24,32,40,48,56,64,72,80,88,96,104,112,120),
                minor_breaks=c(1,8,16,24,32,40,48,56,64,72,80,88,96,104,112,120))+
  theme(plot.margin = unit(c(.2,0,.2,0), "cm"))+
  theme(legend.position=legend_pos,
     legend.direction="horizontal")+
  theme(text = element_text(size = 20))+
  theme(axis.title.y = element_text(margin = margin(t = 0, r = 15, b = 0, l = 10)))+
  theme(axis.title.x = element_text(margin = margin(t = 15, r = 0, b = 10, l = 0)))+
  theme(panel.border = element_rect(linetype = "dashed"))+
  ylim(y_range_down,y_range_up)

# Save all four plots to separate PDFs
ggsave(filename = paste("../final/",f,"_linchart_throughput.pdf",sep=""),linchart,width=8, height = 5.5, units = "in", dpi=300)

}
