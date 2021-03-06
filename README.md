# FlickrFrame

### Overview:
**_flickr_framework.py_** is used to easily and customizable query the official FlickrAPI for georeferenced posts
by supplying the boundaries of an area of interest either through a single bounding box or a GeoJson
file that encorporates multiple bounding boxes. It also allows to simultanously download the corresponding
flickr images by saving them locally in a project folder that is created.

During the process it is checked if a query exceeds the maximum of roughly 4'000 results returned by the API per request.
If that is the case, the same bounding box is queried iteratively with smaller timespans to capture all possible
georeferenced flickr posts from a given region.

The aggregated output is presented in a CSV file with semicolon seperation by default. All the data is UTF-8 encoded and processed if necessary to allow for easy further usage.
The output file is saved in the created project folder and is named according to the current project name, the current time and in the case of a supplied GeoJson file with multiple bounding boxes with the bounding box name.

The workspace or project folder will be established in the same directory as this script file.

Have a look at the required and possible parameters that can be passed while initiating a FlickrFrame instance. For example, by default only Creative Commons Images are returned.

API AUTHENTICATION:
During the FlickrQuerier class invokation a (txt) file has to be provided which contains &lt;KEY> and &lt;SECRET> sections
where the users personal authenticatoin details are contained. E.g.

---

&lt;KEY>

111bc12472b0e18348039184df2343d7

&lt;SECRET>

9f3215cdr17eef7c

---
(does not correspond with real credentials)


Since the FlickrAPI can only be queried with bounding boxes that might not describe the actual research area but rather the envelope of it. The script **_shapefile_clip.py_** can be used to clip .csv files to corresponding shapefiles.
