package me.ele.lpd.dispatch.matrix.score.common.dto.eta;

import lombok.Data;

import java.io.Serializable;
import java.util.Objects;

@Data
public class EtaCacheKey  implements Serializable {
    private static final long serialVersionUID = -3358308219812029434L;
    private final int lngFrom;
    private final int latFrom;
    private final int lngTo;
    private final int latTo;
}
