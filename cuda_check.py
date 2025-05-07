import torch
print(torch.__version__)           # e.g. 2.1.0+cu121
print(torch.cuda.is_available())   # should print True
print(torch.cuda.device_count())   # should be ≥1
print(torch.cuda.get_device_name(0))  # should list your RTX 4080 Super
