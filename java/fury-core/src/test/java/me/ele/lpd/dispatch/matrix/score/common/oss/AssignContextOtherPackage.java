package me.ele.lpd.dispatch.matrix.score.common.oss;

import lombok.Data;
import me.ele.lpd.dispatch.matrix.score.common.dto.eta.Eta;
import me.ele.lpd.dispatch.matrix.score.common.dto.eta.EtaCacheKey;

import java.io.Serializable;
import java.util.Map;
@Data
public class AssignContextOtherPackage  implements Serializable {
    private Map<EtaCacheKey, Eta> etaCache;
}
