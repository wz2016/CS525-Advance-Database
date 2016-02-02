record_mgr: storage_mgr.o dberror.o test_assign3_1.o buffer_mgr.o buffer_mgr_stat.o record_mgr.o expr.o rm_serializer.o
	gcc -o $@ $^ 
ui: storage_mgr.o dberror.o ui.o buffer_mgr.o buffer_mgr_stat.o record_mgr.o expr.o rm_serializer.o
	gcc -o $@ $^ 
storage_mgr.o: storage_mgr.c dberror.h storage_mgr.h  
	gcc -c $<  
dberror.o: dberror.c dberror.h  
	gcc -c $<  
test_assign3_1.o: test_assign3_1.c storage_mgr.h buffer_mgr_stat.h buffer_mgr.h dberror.h test_helper.h
	gcc -c $< 
buffer_mgr.o: buffer_mgr.c buffer_mgr.h storage_mgr.h
	gcc -c $<
expr.o: expr.c expr.h record_mgr.h tables.h dberror.h
	gcc -c $<
record_mgr.o: record_mgr.c record_mgr.h buffer_mgr.h
	gcc -c $<
rm_serializer.o: rm_serializer.c record_mgr.h tables.h dberror.h
	gcc -c $<
buffer_mgr_stat.o: buffer_mgr_stat.c buffer_mgr_stat.h buffer_mgr.h
	gcc -c $<
ui.o: ui.c storage_mgr.h buffer_mgr_stat.h buffer_mgr.h dberror.h test_helper.h
	gcc -c $<

clean: 
	rm -f *.o record_mgr test_table_r test_table_t ui
