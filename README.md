## AUDESOME
Social media represents a rich environment to collect huge amount of data containing useful information on people's behaviors and interactions. This
information is particularly useful in the context of analyzing the mobility of people, where social media posts marked with geographic coordinates or other
information for identifying locations, allow to extract very precise rules on the mobility and movements of people. This paper presents AUDESOME
(AUtomatic Detection of user trajEctories from SOcial MEdia), an automatic method aimed at discovering user mobility patterns from social media posts. In
particular, we have dened two new unsupervised algorithms: i) a text mining algorithm that analyzes the content of posts to automatically extract the main
keywords identifying the Places of Interest (PoI) present in a given area; ii) a clustering algorithm that detects the Regions of Interest (RoIs) starting from the
extracted keywords and geotagged posts of users. We experimentally evaluated the accuracy of AUDESOME taking into account following aspects: i) extraction
the keywords identifying the PoIs; ii) detection of the RoIs; iii) mining of user trajectories. The experiments, performed on a real datasets containing about 3.1
millions of geotagged items published in Flickr in the areas of Rome and Paris, demonstrate that AUDESOME achieves better results than existing techniques.

##HOW TO RUN THE APPLICATION
Run the Main class in /src/main/scale/workflow/Main as a Spark Application.
To compile the project you can use IntelliJ IDE (Scala project + sbt as build tool).