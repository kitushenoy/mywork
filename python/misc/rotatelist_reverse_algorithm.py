#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan 30 14:10:39 2018
@author: kirthi shanbhag
super easy implementation
"""

def test(d=2,a=[1,2,3,4,5]):
    bb=a[:d]
    print( bb)
    cc=a[d:]
    print( cc)
    gg=cc[::-1]
    print(gg)
    print( gg[::-1]+bb[::-1])

test()
