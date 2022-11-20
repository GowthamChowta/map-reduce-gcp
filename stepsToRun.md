1. Open config.ini and set unused ports on your maching for Master, Keyvalue, Mapper, Reducer.

2. To start the Mapper-reducer task. All you need is to invoke the master.py file
```
python3 master.py --noofmappers 3 --noofreducers 2 --task inv_ind --datadir ./data/book_sample.txt
```
* noofmappers -> Is the number of mappers you want to spawn.
* noofreducers -> Is the number of reducers you want to spawn.
* task -> can be inv_ind/wc. inv_ind is to do inverted index. wc is for word count
* datadir -> data file path.

Sample tests to run:  
1. 3 Mappers, 2 Reducers, inverted index task on book_sample.txt file
```
python3 master.py --noofmappers 3 --noofreducers 2 --task inv_ind --datadir ./data/book_sample.txt
```
2. 10 Mappers, 5 Reducers, wc task on book_sample.txt file
```
python3 master.py --noofmappers 10 --noofreducers 5 --task wc --datadir ./data/book_sample.txt
```