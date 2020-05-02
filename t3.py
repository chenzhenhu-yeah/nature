import time
import datetime
import numpy as np
import scipy.stats as si

#
# def d(s,k,r,T,sigma):
#     d1 = (np.log(s / k) + (r + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
#     d2 = d1 - sigma * np.sqrt(T)
#     return (d1,d2)
#
# def delta(s,k,r,T,sigma,n):
#     d1 = d(s,k,r,T,sigma)[0]
#     delta0 = n * si.norm.cdf(n * d1)
#     return delta0
#
#
# def gamma(s,k,r,T,sigma):
#     d1 = d(s,k,r,T,sigma)[0]
#     gamma = si.norm.pdf(d1) / (s * sigma * np.sqrt(T))
#     return gamma
#
# def vega(s,k,r,T,sigma):
#     d1 = d(s,k,r,T,sigma)[0]
#     vega = (s * si.norm.pdf(d1) * np.sqrt(T)) / 100
#     return vega
#
# def theta(s,k,r,T,sigma,n):
#     d1 = d(s,k,r,T,sigma)[0]
#     d2 = d(s,k,r,T,sigma)[1]
#
#     # theta = (-1 * (s * si.norm.pdf(d1) * sigma) / (2 * np.sqrt(T)) - n * r * k * np.exp(-r * T) * si.norm.cdf(n * d2)) / 365
#     theta = (-1 * (s * si.norm.pdf(d1) * sigma) / (2 * np.sqrt(T)) - n * r * k * np.exp(-r * T) * si.norm.cdf(n * d2))
#     return theta
#
# s = 3912
# k = 3900
# r = 2/100
# T = 14/365
# # sigma = 24.59 / 100
# sigma = 25.09 / 100
# n = -1
#
# da = delta(s,k,r,T,sigma,n)
# # print(d)
#
# ga = gamma(s,k,r,T,sigma)
# # print(ga)
#
# va = vega(s,k,r,T,sigma)
# # print(va)
#
# ta = theta(s,k,r,T,sigma,n)
# print(ta)

# d2 = time.strptime('2020-04-03','%Y-%m-%d')
# d1 = time.strptime('2020-04-01','%Y-%m-%d')

d1 = datetime.datetime.strptime('2020-04-01','%Y-%m-%d')
d2 = datetime.datetime.strptime('2020-04-03','%Y-%m-%d')

n = (d2 -d1).days
print(n)
