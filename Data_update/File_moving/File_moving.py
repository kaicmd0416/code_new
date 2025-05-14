import sys
import os
path = os.getenv('GLOBAL_TOOLSFUNC')
sys.path.append(path)
import global_tools as gt
import global_setting.global_dic as glv
class File_moving:
    def data_other_moving(self):
        input = glv.get('input_destination')
        output = glv.get('output_destination')
        gt.folder_creator2(output)
        gt.move_specific_files(input, output)
    def data_product_moving(self):
        input=glv.get('input_prod')
        output=glv.get('output_prod')
        gt.move_specific_files2(input, output)
        print('product_detail已经复制完成')
    def file_moving_update_main(self):
        self.data_other_moving()
        self.data_product_moving()



