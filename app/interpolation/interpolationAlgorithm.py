
#!/usr/bin/env python
def interpolateHI(_list):#[(dist1, h1), (dist2, h2), ... , (distN, hn)]
    return sum(list(map(lambda i: i[1]/i[0], _list)))/sum(list(map(lambda i: 1/i[0], _list)))
    
    
    
    
    
