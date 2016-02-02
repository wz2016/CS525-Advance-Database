storage_mgr: storage_mgr.o dberror.o test_assign1_1.o  
	gcc -o $@ $^  
storage_mgr.o: storage_mgr.c dberror.h storage_mgr.h  
	gcc -c $<  
dberror.o: dberror.c dberror.h  
	gcc -c $<  
test_assign1_1.o: test_assign1_1.c storage_mgr.h  
	gcc -c $<  

clean: 
	rm -f *.o storage_mgr
