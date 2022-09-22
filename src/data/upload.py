from configparser import ConfigParser
import service
import logging
config = ConfigParser()
config.read('SETTING.ini')

def uploadJson(cursor, fileDirectory: str = config['SNOWCONNECTOR']['FILEDIRECTORY']):
    cursor.execute(
        """
        create temporary stage json_temp_int_stage
        file_format = json_format;
        """
    )
    
    cursor.execute("""
    put file://%s/*.json @json_temp_int_stage auto_compress=true;
    """%(fileDirectory))

    cursor.execute("""
        copy into raw_table
        from  @json_temp_int_stage
        file_format = (TYPE = json)
        on_error = 'skip_file';
        """)

    return "Upload Complete"



if __name__ == '__main__':
    cursor = service.engine.connect()
    uploadJson(cursor)