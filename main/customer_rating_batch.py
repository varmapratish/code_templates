# core python imports
import os, sys
from datetime import date

# add parent package to sys.path
sys.path.append(os.path.join(os.path.dirname(__file__), '..','..'))

# app specific imports
from ML.projects.customer_rating_project.customer_rating_mailer import customerrating

# if date.today().weekday() == 0:
model = customerrating()
model.calculate_customer_rating()
model.mailer()
