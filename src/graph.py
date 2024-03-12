#!/usr/bin/env python3
from brand import Brand
import sys

if __name__ == '__main__':
    brand = Brand(int(sys.argv[1]))
    try:
        brand.make_graph(path='PER.png',plot='PER')
    except:
        import traceback
        traceback.print_exc()
    try:
        brand.make_graph(path='averagePER.png',plot='averagePER')
    except:
        import traceback
        traceback.print_exc()
    try:
        brand.make_graph(path='ROE.png',plot='ROE')
    except:
        import traceback
        traceback.print_exc()