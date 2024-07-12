# Assignment of Homework 3

In this homework, you will run a Spark program on the *CloudVeneto* cluster.
The core of the Spark program will be the implementation of **2-round coreset-based MapReduce algorithm for k-center with z outliers**,
which works as follows:

1. in Round 1, separately for each partition of the dataset, a weighted coreset of k+z+1 points is computed,
where the weight assigned to a coreset point p is the number of points in the partition which are closest to p (ties broken arbitrarily);
2. in Round 2, the L weighted coresets (one from each partition) are gathered into one weighted coreset of size (k+z+1)*L, and one reducer runs 
the sequential algorithm developed for Homework 2 (SeqWeightedOutliers) on this weighted coreset to extract the final solution.

In the homework you will test the accuracy of the solution and the scalability of the various steps.

The [.docx file](https://github.com/stefanobinotto/Big-Data-Computing-course/blob/main/Homework%203/G064HW3table.docx) describes the results and comparisons with different settings and configurations.
