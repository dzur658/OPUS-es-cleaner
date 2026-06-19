FROM nvcr.io/nvidia/nemo-curator:25.09

# manually patch gpustat to handle "??" values properly on GB10 unified memory boxes
# without this patch gpustat will crash the entire pipeline when NVML returns "??" for available VRAM
RUN sed -i "s/return int(self.entry\['memory.used'\])/v = self.entry.get('memory.used'); return 0 if v in (None, '??') else int(v)/g" /opt/venv/lib/python*/site-packages/gpustat/core.py \
 && sed -i "s/return int(self.entry\['memory.total'\])/v = self.entry.get('memory.total'); return 0 if v in (None, '??') else int(v)/g" /opt/venv/lib/python*/site-packages/gpustat/core.py

WORKDIR /workspace
COPY src/ ./src/

CMD ["bash"]