# Weijia Jin, Jialiang Zhang
# Adapted from Daniel Dorling and Zachary Forest Johnson's code for generating Dorling cartograms.
# Generalized algorithm to accept all data with geographic attributes (lat, long) and modified algorithm
# for better results. Paper: http://goo.gl/NTo0Os

import math
import json
# import numpy as np
# from scipy.optimize import minimize


DATA_PATH = '../data/LasVegasData_final.json'
NEIGHBORS_FILE = '../data/LasVegasNeighbors.json'
ANIMATION_STEPS = 10

def load_data_from_json(path):
    with open( path ) as f:      
        value = f.read().replace('\n', '')
        data = json.loads(value)
    return data

def make_json_from_dict(filename, dt):
    with open(filename, 'w') as fp:
        json.dump(dt, fp)

def objectivefn(xar):
    optimal = 10e10
    best = -1
    for i,x in enumerate(xar): 
        cartogram = Cartogram('count', ratio=0.4, friction=0.50, tolerance=0.001, displacement_tolerance=0.001,
            positionratio=x, positiontolerance=0.01)
        objective = cartogram.make()[0]
        # objective = objective[0]
        if objective < optimal:
            optimal = objective
            best = i
        print 'ratio: %.4f, objective: %.8f' % (x,objective)
    return xar[best]
    

def main():
    # data = load_data_from_json(DATA_PATH)
    choice = 0
    #
    # Original Dorling C code, same constants
    #
    if choice == 0:
        cartogram = Cartogram('count', tolerance=0.001, displacement_tolerance=0.001, positiontolerance=0.0)
        out = cartogram.make()
        print out[0]
    # cartogram = Cartogram('count', friction=0.75)

    #
    # Modified constants
    #
    elif choice == 1:
        cartogram = Cartogram('count', ratio=0.4, friction=0.5, tolerance=0.001, displacement_tolerance=0.001,
            positionratio=1.3, positiontolerance=0.01)
        out = cartogram.make()
        print out[0]
    elif choice == 2:
        test_ratios = [x/10.0 for x in range(1,0)]
        print test_ratios
        best_var = objectivefn(test_ratios)
        cartogram = Cartogram('count', ratio=0.4, friction=0.50, tolerance=0.001, displacement_tolerance=0.001,
            positionratio=best_var, positiontolerance=0.01)
        # 1.3
        out = cartogram.make()
        print best_var, out[0]

    # x0 = np.array([0.5])
    # res = minimize(objectivefn, x0)
    # print(res.x)
    # cartogram = Cartogram(data, 'count', 'polygon')
    # output = cartogram.make()
    # # make_json_from_dict('results.json', output)
    # print output

    make_json_from_dict('results.json', out[1])


class Cartogram(object):
    """

    Example usage:
        
        >> from dorling import Cartogram
        >> queryset = Geography.objects.all()
        >> cartogram = Cartogram(queryset, 'population')
        >> enhanced_queryset = cartogram.make()
    
    Based on code published by Zachary Forest Johnson
    
        http://indiemaps.com/blog/2008/01/dorlingpy/
    
    """
    def __init__(self, data_attr, iterations=1000, friction=0.25, ratio=0.4, optimize_ratio=0.4,
        tolerance=0.0, positionratio=0.0, displacement_tolerance=0.0, positiontolerance=0.0):

        self.queryset = load_data_from_json(DATA_PATH)
        self.items = self.queryset.keys()
        self.index_to_key = {}
        for i,k in enumerate(self.queryset):
            self.queryset[k]['i'] = i
            self.index_to_key[i]= k 
        self.positionratio = positionratio
        self.data_attr = data_attr
        self.iterations = iterations
        self.n = len(self.queryset)
        self.friction = friction
        self.ratio = ratio

        self.optimize_ratio = optimize_ratio
        self.positiontolerance = positiontolerance
        self.displacement_tolerance = displacement_tolerance
        self.tolerance = tolerance
        self.lower_threshold = 0 - tolerance
        self.upper_threshold = 0 + tolerance
        self.widest_radius = 0.0
        self.x_vectors = dict((i, 0.0) for i in range(self.n))
        self.y_vectors = dict((i, 0.0) for i in range(self.n))
        self.x_values = dict((i, self.queryset[k]['longitude']) for i, k in enumerate(self.queryset))
        self.y_values = dict((i, self.queryset[k]['latitude']) for i, k in enumerate(self.queryset))
        self.data_values = dict((i, self.queryset[k][data_attr]) for i, k in enumerate(self.queryset))
        # self.perimeter_values = dict((i, 0.0) for i in range(self.n))
        # self.border_values = dict((i, {}) for i in range(self.n))
        self.total_distance = 0.0
        self.total_radius = 0.0
        self.radius_values = {}
        self.tree = {}
        self.end_pointer = 1
        self.scale = None
        self.neigbors_within_distance = 0

        #
        # We use inverse distance between neighbors to substitute for border lengths and perimeters
        #
        self.proximities = {}
        for k in self.items:
            obj = self.queryset[k]
            inverse_distances = []
            for n in self.items:
                if n != k:
                    other = self.queryset[n]
                    dist = self.compute_obj_distance(obj, other)
                    inverse_distances.append(1.0/dist)
                # self.inverse_distances[(obj['i'], other['i'])] = 1.0/dist
            # ordered = [i[0] for i in sorted(enumerate(neighbor_distances), key=lambda x:x[1])]
            # L = math.sqrt( sum( [x**2 for x in inverse_distances] ) )
            total = sum(inverse_distances)
            proximity_ratios = [ x/total for x in inverse_distances ] # normalized
            # print 'inverse_distances: ', inverse_distances
            i = 0
            for n in self.items:
                if n != k:
                    other = self.queryset[n]
                    self.proximities[(obj['i'], other['i'])] = proximity_ratios[i]
                    i += 1

        self.out_data = {}
        self.canvas = [0,0,0,0]
        self.mylist = {}

    def compute_distance(self, i, j):
        xd = self.x_values[i]-self.x_values[j]
        yd = self.y_values[i]-self.y_values[j]
        return math.sqrt(xd * xd + yd * yd)

    def compute_obj_distance(self, obj, other):
        return self.compute_distance(obj['i'], other['i'])        

    def isneighbor(self, i, j):
        key_i = self.index_to_key[i]
        key_j = self.index_to_key[j]
        item_j = self.queryset[key_j]
        if key_i in item_j['neighbors']:
            return True
        else:
            return False

    def eval_constraints(self):
        collisions = 0
        total_collisions = 0
        broken_links = 0
        total_links = 0
        total_position_off = 0
        for i in xrange(self.n-1):
            for j in xrange(i+1,self.n):
                dist = self.compute_distance(i,j)
                space = dist - self.radius_values[i] - self.radius_values[j]
                total_collisions += 1
                if space < self.lower_threshold:
                    collisions += 1
                if self.isneighbor(i,j):
                    total_links += 1
                    if space > self.upper_threshold:
                        broken_links += 1
            obj = self.queryset[self.index_to_key[i]]
            xd = obj['longitude'] - self.x_values[obj['i']]
            yd = obj['latitude'] - self.y_values[obj['i']]
            posdist = math.sqrt(xd * xd + yd * yd)
            if posdist > self.positiontolerance: 
                total_position_off += 1
        return self.optimize_ratio*broken_links + (1-self.optimize_ratio)*collisions + 0.2*total_position_off


    def make(self):
    
        #
        # Determine the neighbors of each object and our global scale
        #
        x_max = -10e10
        x_min = -x_max
        y_max = x_max
        y_min = -x_max        
        neighbors = load_data_from_json(NEIGHBORS_FILE)

        # for k in self.items:
        #     r = math.log(self.queryset[k][self.data_attr])
        #     if r == 0.0:
        #         r = 0.4
        #     self.queryset[k][self.data_attr] = r

        for k in self.items:
            obj = self.queryset[k]
            this_data = obj[self.data_attr]

            #
            # Todo: automate list of neighbors, guess by distance
            #
            neighbors_k = neighbors[k]
            obj['neighbors'] = filter(lambda x: x in self.items, neighbors_k)

            for other_k in obj['neighbors']:
                other = self.queryset[other_k]
                other_data = other[self.data_attr]
                self.total_distance += self.compute_obj_distance(obj, other)
                self.total_radius += math.sqrt(this_data/math.pi) + math.sqrt(other_data/math.pi)
        self.scale = self.total_distance / self.total_radius
        # print self.scale
        
        #
        # Calculate the radii we'll start with
        #
        self.out_data['radius'] = {}
        for k in self.queryset:
            obj = self.queryset[k]
            this_radius = self.scale * math.sqrt(obj[self.data_attr]/math.pi)
            # print '%s: %.6f' % (k, this_radius)
            self.radius_values[obj['i']] = this_radius
            self.out_data['radius'][k] = this_radius
            obj['radius'] = this_radius
            if this_radius > self.widest_radius:
                self.widest_radius = this_radius
        
        #
        # Make the moves
        #        
        out_idx = -1
        for iteration in range(self.iterations):
            self.end_pointer = 1
            self.tree = dict((i+1, {'id': 0}) for i in range(self.n))
            [self.add_point(1, 1, self.queryset[k]) for k in self.queryset]
            # self.out_data[iteration] = {}
            displacement = 0.0

            #
            # For each circle
            #
            for k in self.queryset:
                obj = self.queryset[k]
                self.neighbors_within_distance = 0
                distance = self.widest_radius + obj['radius']
                self.get_point(1, 1, obj, distance)
                xrepel = yrepel = 0.0
                xattract = yattract = 0.0
                closest = self.widest_radius

                #
                # Work out repelling force of overlapping neighbours
                #
                if self.neighbors_within_distance > 0:
                    # print "repel in iter%d: %d" % (iteration, self.neighbors_within_distance)

                    #
                    # For each neighbor
                    #
                    for i in range(self.neighbors_within_distance):
                        other_i = self.mylist[i]
                        if other_i != obj['i']:
                            dist = self.compute_distance(other_i, obj['i'])
                            if dist < closest:
                                closest = dist

                            #
                            # Positive only when circles overlap, negative when there is empty space between them
                            #
                            overlap = obj['radius'] + self.radius_values[other_i] - dist
                            # print 'overlap %d: %.6f' % (i, overlap)
                            if overlap > 0.0:
                                # print '%d overlap %d: %.6f' % (iteration, i, overlap)                                
                                # print 'dist %d: %.6f' % (i, dist)                                    
                                xrepel = xrepel - overlap*(self.x_values[other_i]-self.x_values[obj['i']])/dist
                                yrepel = yrepel - overlap*(self.y_values[other_i]-self.y_values[obj['i']])/dist
                                # if k == '89146': print 'x,yrepel: %.6f, %.6f' % (xrepel, yrepel)

                #
                # Work out forces of attraction between neighbours
                #
                for other_k in obj['neighbors']:
                    other_i = self.queryset[other_k]['i']
                    dist = self.compute_distance(obj['i'], other_i)
                    overlap = dist - obj['radius'] - self.radius_values[other_i]
                    if overlap > 0.0:
                        overlap = overlap * self.proximities[(obj['i'], other_i)]
                        xattract = xattract + overlap*(self.x_values[other_i]-self.x_values[obj['i']])/dist
                        yattract = yattract + overlap*(self.y_values[other_i]-self.y_values[obj['i']])/dist

                #
                # Work out force of attraction to original position
                #
                xd = obj['longitude'] - self.x_values[obj['i']]
                yd = obj['latitude'] - self.y_values[obj['i']]
                posdist = math.sqrt(xd * xd + yd * yd)
                if posdist > 0:
                    # xattract += self.positionratio * xd / posdist
                    # yattract += self.positionratio * yd / posdist
                    xattract += self.positionratio * xd * posdist/self.radius_values[obj['i']]
                    yattract += self.positionratio * yd * posdist/self.radius_values[obj['i']]

                #
                # Now work out the combined effect of attraction and repulsion 
                #
                atrdst = math.sqrt(xattract * xattract + yattract * yattract)
                repdst = math.sqrt(xrepel * xrepel + yrepel * yrepel)
                # print 'closest: %.6f' % closest
                # print 'xrepel, yrepel: %.6f, %.6f' % (xrepel, yrepel)
                # print 'xattract, yattract: %.6f, %.6f' % (xattract, yattract)
                # print 'atrdst, repdst, closest: %.6f, %.6f, %.6f' % (atrdst, repdst, closest)
                base = 1.0
                # if atrdst == 0: base = 1.0
                if repdst > closest:
                    xrepel = closest * xrepel / (repdst + base)
                    yrepel = closest * yrepel / (repdst + base)
                    repdst = closest
                if repdst > 0.0:
                    xtotal = (1.0-self.ratio) * xrepel + self.ratio*(repdst*xattract/(atrdst + base))
                    ytotal = (1.0-self.ratio) * yrepel + self.ratio*(repdst*yattract/(atrdst + base))
                else:
                    if atrdst > closest:
                        xattract = closest * xattract/(atrdst+base)
                        yattract = closest * yattract/(atrdst+base)
                    xtotal = xattract
                    ytotal = yattract
                self.x_vectors[obj['i']] = self.friction * (self.x_vectors[obj['i']] + xtotal)
                self.y_vectors[obj['i']] = self.friction * (self.y_vectors[obj['i']] + ytotal)
                displacement += math.sqrt(xtotal*xtotal + ytotal*ytotal)

            #
            # Update the position for each object
            #
            c = 0.0
            write = False
            if iteration%ANIMATION_STEPS == 0:
                out_idx += 1
                self.out_data[out_idx] = {}
                write = True
                # print 'Displacement in iter %d: %.6f' % (iteration, displacement)


            for k in self.queryset:
                obj = self.queryset[k]
                xchange = self.x_vectors[obj['i']] + c
                ychange = self.y_vectors[obj['i']] + c
                self.x_values[obj['i']] += xchange
                self.y_values[obj['i']] += ychange

                if write: 
                    self.out_data[out_idx][k] = [self.x_values[obj['i']], self.y_values[obj['i']]]
                    x_max = max(self.x_values[obj['i']], x_max)
                    x_min = min(self.x_values[obj['i']], x_min)
                    y_max = max(self.y_values[obj['i']], y_max)
                    y_min = min(self.y_values[obj['i']], y_min)
                
                # print '%s: %.6f, %.6f' % (k, self.x_values[obj['i']], self.y_values[obj['i']])

                # if (self.x_vectors[obj['i']] < 0.0001) and (self.y_vectors[obj['i']] < 0.0001):
                #     print "%d: Zero Change for %s" % (iteration, k)

                # if iteration > 0 and k == '89146':
                #     print 'Changes for iter%d: %.6f, %.6f' % (iteration, xchange, ychange)
                #     xd = self.x_values[obj['i']] - self.last_x[obj['i']]
                #     yd = self.y_values[obj['i']] - self.last_y[obj['i']]
                    # print '%d Change: %.6f, %.6f' % (iteration, xd, yd)

            #
            # Evaluate using number of overlapping circles and non-touching neighbors
            #

            # broken_links = 0
            # total = 0.0
            # total_space = 0.0
            # total_overlap = 0.0
            # for i in xrange(self.n-1):
            #     # item = self.queryset[all_keys[i]]
            #     k = self.index_to_key[i]
            #     obj = self.queryset[k]
            #     # xd = self.x_values[i]-obj['Long']
            #     # yd = self.y_values[i]-obj['Lat']
            #     # posdist = math.sqrt(xd * xd + yd * yd)
            #     for j in xrange(i+1,self.n):
            #         # other = self.queryset[all_keys[j]]
            #         dist = self.compute_distance(i,j)
            #         space = dist - self.radius_values[i] - self.radius_values[j]
            #         # self.upper_threshold = 0.001  
            #         if self.isneighbor(i,j):
            #             total += 1                  
            #             if space > self.upper_threshold: 
            #                 # print 'Extra: %.6f' % (space/dist)
            #                 total_space += space - self.upper_threshold
            #                 broken_links += 1
            #         if space < self.lower_threshold:
            #             total_overlap += self.lower_threshold - space
            #             collisions += 1

            optimize_value = self.eval_constraints()
            # print 'Optimize in iter %d: %.6f' % (iteration, optimize_value)

            # print 'Iter%d Collisions: %d/%d' % (iteration, collisions, total_collisions)
            # print 'Iter%d Broken links: %d/%d' % (iteration, broken_links, total_links)
            # print displacement, self.displacement_tolerance
            if displacement <= self.displacement_tolerance:
                break

        # print 'ranges: %.6f, %.6f, %.6f, %.6f' % (x_max, x_min, y_max, y_min)
        self.out_data['limitation'] = [x_min, y_min, x_max, y_max]
        # Pass it back out
        return (optimize_value, self.out_data)
        # return optimize

    def add_point(self, pointer, axis, obj):
        if self.tree[pointer]['id'] == 0:
            self.tree[pointer]['id'] = obj['i']+1
            self.tree[pointer]['left'] = 0
            self.tree[pointer]['right'] = 0
            self.tree[pointer]['xpos'] = self.x_values[obj['i']]
            self.tree[pointer]['ypos'] = self.y_values[obj['i']]
        else:
            if axis == 1:
                if self.x_values[obj['i']] >= self.tree[pointer]['xpos']:
                    if self.tree[pointer]['left'] == 0:
                        self.end_pointer += 1
                        self.tree[pointer]['left'] = self.end_pointer
                    self.add_point(self.tree[pointer]['left'], 3-axis, obj)
                else:
                    if self.tree[pointer]['right'] == 0:
                        self.end_pointer += 1
                        self.tree[pointer]['right'] = self.end_pointer
                    self.add_point(self.tree[pointer]['right'], 3-axis, obj)
            else:
                if self.y_values[obj['i']] >= self.tree[pointer]['ypos']:
                    if self.tree[pointer]['left'] == 0:
                        self.end_pointer += 1
                        self.tree[pointer]['left'] = self.end_pointer
                    self.add_point(self.tree[pointer]['left'], 3-axis, obj)
                else:
                    if self.tree[pointer]['right'] == 0:
                        self.end_pointer += 1
                        self.tree[pointer]['right'] = self.end_pointer
                    self.add_point(self.tree[pointer]['right'], 3-axis, obj)

    def get_point(self, pointer, axis, obj, distance):
        if pointer > 0:
            if self.tree[pointer]['id'] > 0:
                if axis == 1:
                    if self.x_values[obj['i']]-distance < self.tree[pointer]['xpos']:
                        self.get_point(self.tree[pointer]['right'], 3-axis, obj, distance)
                    if self.x_values[obj['i']]+distance >= self.tree[pointer]['xpos']:
                        self.get_point(self.tree[pointer]['left'], 3-axis, obj, distance)
                if axis == 2:
                    if self.y_values[obj['i']]-distance < self.tree[pointer]['ypos']:
                        self.get_point(self.tree[pointer]['right'], 3-axis, obj, distance)
                    if self.y_values[obj['i']]+distance >= self.tree[pointer]['ypos']:
                        self.get_point(self.tree[pointer]['left'], 3-axis, obj, distance)
                if (self.x_values[obj['i']]-distance < self.tree[pointer]['xpos'] and
                    self.x_values[obj['i']]+distance >= self.tree[pointer]['xpos']):
                    if (self.y_values[obj['i']]-distance < self.tree[pointer]['ypos'] and
                        self.y_values[obj['i']]+distance >= self.tree[pointer]['ypos']):
                            self.mylist[self.neighbors_within_distance] = self.tree[pointer]['id']-1
                            self.neighbors_within_distance += 1


#
# Runs code
#
main()

