## Output

The goal is to create a single `json` file that can be used as the lookup source for `typeahead` (https://twitter.github.io/typeahead.js/) input box on a web page.

The information will be in the following format:
```
[
...
{"name": "Paris (All Airports)","code": "PAR","country": "FR","cityname": "Paris","value": "PAR","tokens": ["Paris","PAR","FR"]},
...
{"name": "Villacoublay Velizy","code": "VIY","country": "FR","cityname": "Paris","value": "VIY","tokens": ["VIY","PAR","Velizy","Paris","FR","Villacoublay"]}
...
]
```

The following code is an excerpt from an `ASPX` page using `typeahead` in `jQuery`.
```javascript
$(function () {
                // apply typeahead to the text input box with id
                $('#<%= txtSource.ClientID%>').typeahead({
                    name: 'airports-all',
                    // data source
                    prefetch: 'Globals/airports_complete_cityname.json',
                    limit: 15,
                    // template for each suggestion
                    template: [
                        '<p class="airport-code">{{code}}</p>',
                        '<p class="airport-name">{{name}}</p>',
                        '<p class="airport-city">{{cityname}}</p>',
                        '<p class="airport-country">{{country}}</p>'
                    ].join(''),
                    // template engine
                    engine: Hogan
                });
            }); 
```

The output will be as per the following examples:

* Lookup by name of the city \
![city_name.png](https://raw.githubusercontent.com/alimaken/airports/master/data/screenshots/city_name.PNG) \
![city_name.png](https://raw.githubusercontent.com/alimaken/airports/master/data/screenshots/city_name_2.PNG) \
* Lookup by 2 character country code \
![city_name.png](https://raw.githubusercontent.com/alimaken/airports/master/data/screenshots/country_code.PNG) \
* Lookup by any character in any field \
![city_name.png](https://raw.githubusercontent.com/alimaken/airports/master/data/screenshots/anything.PNG)

## Input

All the input files are taken from TravelPort (https://developer.travelport.com/app/developer-network/resource-centre-uapi).
You might need to create an account to get access to the `Reference Data Tables` section.

* The schema of airports.csv:

```bash
root
 |-- Code: string (nullable = true)
 |-- Synonym: string (nullable = true)
 |-- Name: string (nullable = true)
 |-- Country: string (nullable = true)
 |-- State: string (nullable = true)
 |-- MetroCode: string (nullable = true)
 |-- City: string (nullable = true)
 |-- Type: integer (nullable = true)
 |-- IsCommercial: string (nullable = true)
 |-- unknown: integer (nullable = true)
 ```

* The schema of major_cities.csv (Not a part of the Reference Data available on the TravelPort site):

```bash
root
 |-- City: string (nullable = true)
 |-- Code: string (nullable = true)
 |-- Name: string (nullable = true)
 |-- Country: string (nullable = true)
 |-- Airports: string (nullable = true)
```

These cities are processed manually in this case. \
The output can be found in [airport_major_cities.json](https://github.com/alimaken/airports/blob/master/data/output/airports_major_cities.json)
The contents are simply merged with the output of this project.

* The schema of cities.csv:

```bash
root
 |-- CityCode: string (nullable = true)
 |-- Synonym: string (nullable = true)
 |-- Name: string (nullable = true)
 |-- Country: string (nullable = true)
 |-- State: string (nullable = true)
 |-- MetroCode: string (nullable = true)
 |-- Airports: string (nullable = true)
 |-- IsHost: string (nullable = true)
 |-- IsCommercialService: string (nullable = true)
```


## PROCESS
1. Filter `airports` where type is 1, 2 or 3. The types range from 1 to 9, major to minor respectively.
2. Correlate `cities` to itself based on Synonymous records 
3. Correlate `airports` to itself based on Synonymous records 
4. Correlate final `airports` and final `cities` based on City `code`
5. Write output to a file in the required (`typeahead` template) `JSON` format   

## Related Info
* How to setup winutils on Windows to create an output file:
[jaceklaskowski gitbooks link](https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-tips-and-tricks-running-spark-windows.html)