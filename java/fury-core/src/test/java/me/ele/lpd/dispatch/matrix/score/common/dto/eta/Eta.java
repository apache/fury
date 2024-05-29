package me.ele.lpd.dispatch.matrix.score.common.dto.eta;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
public class Eta  implements Serializable {
    /** 单位米 */
    private int distance;
    /**
     * 单位秒
     */
    private int time;
    /**
     * 远端ETA数据来源
     */
    private String etaType;
    /**
     * 围栏id
     */
    private Integer fenceId;
    /**
     * 围栏类型
     */
    private String fenceType;
    /**
     * eta数据来源
     */
    private SourceEnum source;

}
