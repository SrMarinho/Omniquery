from .output import Output, DatabaseOutput, FileOutput, APIOutput

Output.register(DatabaseOutput)
Output.register(FileOutput)
Output.register(APIOutput)
