import urllib.request
from bs4 import BeautifulSoup
from selenium import webdriver
import time
import pandas as pd

options = webdriver.firefox.options.Options()
options.headless = False
driver = webdriver.Firefox(firefox_options=options, executable_path = '../geckodriver/geckodriver')

loginpage = f"http://aqualim.environnement.wallonie.be"
print(loginpage)
driver.get(loginpage)
# time.sleep(1)

# mappage = f"http://aqualim.environnement.wallonie.be/GeneralPages.do?method=displayStationsMap"
# print(mappage)
# driver.get(mappage)
# time.sleep(3)

listpage = "http://aqualim.environnement.wallonie.be/GeneralPages.do?method=displayStationsList"
print(listpage)
driver.get(listpage)

#   <div class="dataTables_length" id="dataTable_length"><label>Afficher <select name="dataTable_length" aria-controls="dataTable" class="form-control input-sm"><option value="15">15</option><option value="25">25</option><option value="50">50</option><option value="55">ff</option><option value="-1">Tout</option></select> lignes par page</label></div> 
select_datatable_length = webdriver.support.ui.Select(driver.find_element_by_name("dataTable_length"))
select_datatable_length.select_by_value("-1")

#  <table id="dataTable" class="table table-striped table-bordered table-hover dataTable no-footer" cellspacing="0" width="100%" role="grid" aria-describedby="dataTable_info" style="width: 100%;">
# 	<thead>  <tr role="row"><th class="sorting_desc" tabindex="0" aria-controls="dataTable" rowspan="1" colspan="1" aria-label="Bassin versant: activate to sort column ascending" style="width: 414px;" aria-sort="descending">Bassin versant</th><th class="sorting" tabindex="0" aria-controls="dataTable" rowspan="1" colspan="1" aria-label="Rivière: activate to sort column ascending" style="width: 334px;">Rivière</th><th class="sorting" tabindex="0" aria-controls="dataTable" rowspan="1" colspan="1" aria-label="Localité Station: activate to sort column ascending" style="width: 402px;">Localité<span class="hidden-xs"> Station</span></th><th class="hidden-xs sorting" tabindex="0" aria-controls="dataTable" rowspan="1" colspan="1" aria-label="N° Station: activate to sort column ascending" style="width: 197px;">N° Station</th></tr> 			</thead>
datatable = driver.find_element_by_id("dataTable")
print(datatable.get_attribute('outerHTML'))

df = pd.read_html(datatable.get_attribute('outerHTML'), index_col=3)

print(df)


time.sleep(6)
driver.quit()
