from etl.airbnb_scraper import AirBnBScraper

airbnb_scraper = AirBnBScraper(start_date='2019-12-01')
airbnb_scraper.execute()
