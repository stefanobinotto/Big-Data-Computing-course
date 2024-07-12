# Assignment of Homework 2

Develop a method **SeqWeightedOutliers(P,W,k,z,alpha)** which implements the weighted variant of *kcenterOUT* (the 3-approximation algorithm for k-center with z-outliers).
The method takes as input the set of points P, the set of weights W, the number of centers k, the number of outliers z, and the coefficient alpha used by the algorithm,
and returns the set of centers S computed as specified by the algorithm.
It is understood that the i-th integer in W is the weight of the i-th point in P.
Java users: represent P and S as ArrayList<Vector>, and W as  ArrayList<Long>. Python users: represent P and S as list of tuple and W as list of integers.
Considering the algorithm's high complexity, try to make the implementation as efficient as possible. 

Develop a method **ComputeObjective(P,S,z)** which computes the value of the objective function for the set of points P, the set of centers S, and z outliers (the number of centers, which is the size of S, is not needed as a parameter).
Hint: you may compute all distances d(x,S), for every x in P, sort them, exclude the z largest distances, and return the largest among the remaining ones. Note that in this case we are not using weights!
Write a program GxxxHW2.java (for Java users) or GxxxHW2.py (for Python users), where xxx is your 3-digit group number (e.g., 004 or 045), which receives in input the following command-line (CLI) arguments: 
A path to a text file containing point set in Euclidean space. Each line of the file contains, separated by commas, the coordinates of a point. Your program should make no assumptions on the number of dimensions!
An integer k (the number of centers).
An integer z (the number of allowed outliers).

The program must do the following:
1. Read the points in the input file into an ArrayList<Vector> (list of tuple in Python) called inputPoints. To this purpose, you can use the code provided in the file InputCode.java, for Java users, and InputCode.py, for Python users.
2. Create an ArrayList<Long> (list of integer in Python) called weights of the same cardinality of inputPoints, initialized with all 1's. (In this homework we will use unit weights, but in Homework 3 we will need the generality of arbitrary integer weights!).
3. Run SeqWeightedOutliers(inputPoints,weights,k,z,0) to compute a set of (at most) k centers. The output of the method must be saved into an ArrayList<Vector> (list of tuple in Python) called solution.
4. Run ComputeObjective(inputPoints,solution,z) and save the output in a variable called objective.
5. Return as output the following quantities: n =|P|, k, z, the initial value of the guess r, the final value of the guess r, and the number of guesses made by SeqWeightedOutliers(inputPoints,weights,k,z,0), the value of the objective function (variable objective), and the time (in milliseconds) required by the execution of SeqWeightedOutliers(inputPoints,weights,k,z,0).
   IT IS IMPORTANT THAT ALL PROGRAMS USE THE SAME OUTPUT FORMAT AS IN THE FOLLOWING EXAMPLE: OutputFormat.txt
8. Test your program using the datasets available here.
