# Data Analysis Using Map/Reduce On IMDB Dataset

IMDB is a dataset containing information about movies (international) and TV episodes from their
beginnings. The information includes movie titles, directors, actors, genre, and year
produced/started. Utilize the ability of Map/Reduce paradigm to analyze the IMDB dataset.

<h2>Query</h2>
A Map/Reduce program to compute the number of highly rated movies with
different genre combinations for pre-defined periods. <br>


1. 3 disjoint 10-year periods are being explored as a part of this project – [1991-2000], [2001-
2010], and [2011-2020] <br>
2. Genre Combinations to be explored <br>
  - Action, Thriller <br>
  - Adventure, Drama <br>
  - Comedy, Romance <br>
  
Note: For the genre combination like Comedy, Romance you
need to consider the movies with a rating of at least 7.5 and which have at least these two
genres present in their corresponding list of genres. For example, movies with
“Comedy,Drama,Romance”, “Comedy,Family,Romance”, “Comedy,Romance, etc. satisfy the
requirement.
