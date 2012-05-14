Strategy used to handle liers
---------------------------------------------
1. Liers can be identified only if the number of liers voted is less than the number of update servers voted.
2. To ensure above constraint is maintained the GET and PUT quorum has been updated. 
3. Lets say atmost NL_MAX users can lie then
    for GET to properly work client needs to have atleast 2*NL_MAX + 1 votes and since there can be NL_MAX liers so we need to have NL_MAX+1 votes from updated servers(servers which contain updated data). So read and write operlap should contain NL_MAX+1 updated servers. To achieve this we can change the NR+NW > N constraint to NR+NW > N + NL_MAX which guarnetees NL_MAX + 1 overlap between reads and writes. Also, NR > 2*NL_MAX to ensure majority of updated servers.
    for PUT to properly work properly
		 NW - NR > NL_MAX  
                =>  NW - (N-NW) > 2*NL_MAX
                =>  2*NW - N > 2* NL_MAX
                =>  NW > (2*NL_MAX + N)/2

4. In the client program based on the given arguments for NR, NW and N we are calculating maximum number of leirs that can be identified. This is printed as the very first line of the output. 
5. And even in case if there are actually more liers the number of liers identified will only depend on NR, NW and N.
6. We have used a Time out value of 60 seconds. So if the any of the read operation that takes more than 60 seconds then the operation will not be commited this is more relavent in case of PUT request where the commit operation might take time then if client takes more than 60 seconds then operation will be aborted.
