from pyspark import SparkContext, SparkConf


config = SparkConf().setMaster('local').setAppName('BFS')
sc = SparkContext(conf = config)
sc.setLogLevel("ERROR")


startHero = 5306
targetHero = 14

hitCounter = sc.accumulator(0)

# for the convenience of the computation,
# I use 1 represents the unprocessed node, 2 represents processing node, 3 represents processed node, seperately. 

def convert2Graph(line):
	heroes = line.split()
	node = int(heroes[0])
	neighbors = list(map(int,heroes[1:]))

	# print('number of neighbors is {}'.format(len(neighbors)))

	status = 1
	distance = 999

	if node == startHero:
		status = 2
		distance = 0

	return (node, (neighbors, status, distance))


def getInitialRdd():
	rawData = sc.textFile('/root/spark_course/Marvel-Graph.txt')
	return rawData.map(convert2Graph)


def mapper(node):
	hero_ID = node[0]
	features = node[1]

	neighbors = features[0]
	status = features[1]
	distance = features[2]

	tmp = []

	if status == 2:
		newDistance = distance+1
		newStatus = 2
		newNeighbor = []
		for nb in neighbors:
			if nb == targetHero:
				hitCounter.add(1)

			tmp.append((nb, (newNeighbor, newStatus, newDistance)))
		
		tmp.append((hero_ID, (neighbors, 3, distance)))
		return tmp
	else:
		tmp.append((hero_ID, (neighbors, status, distance)))
		return tmp


def reducer(feature1, feature2):
	neighbors1 = feature1[0]
	status1 = feature1[1]
	distance1 = feature1[2]

	neighbors2 = feature2[0]
	status2 = feature2[1]
	distance2 = feature2[2]

	newNeighbor = []
	if len(neighbors1)>0:
		newNeighbor = neighbors1 + newNeighbor
	if len(neighbors2)>0:
		newNeighbor = neighbors2 + newNeighbor

	newDistance = min(distance1,distance2)

	newStatus = max(status1, status2)

	return (newNeighbor, newStatus, newDistance)


rdd = getInitialRdd()
print('Intial rdd count: {}'.format(rdd.count()))

for i in range(0,10):
	print('Run Breath_First_Search at Round {}'.format(i))
	print('Current rdd size is: {}'.format(rdd.count()))

	mapped = rdd.flatMap(mapper)

	print('Processing {} nodes'.format(mapped.count()))

	if hitCounter.value > 0:
		print('Target Hero has been hit from {} directions'.format(hitCounter.value))
		break

	rdd = mapped.reduceByKey(reducer)










