package org.min.watergap.outfall.incre;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.min.watergap.common.context.WaterGapContext;
import org.min.watergap.common.exception.ErrorCode;
import org.min.watergap.common.exception.WaterGapException;
import org.min.watergap.common.utils.CollectionsUtils;
import org.min.watergap.outfall.OutFallDrainer;
import org.min.watergap.piping.thread.SingleThreadWorkGroup;
import org.min.watergap.piping.translator.PipingData;
import org.min.watergap.piping.translator.impl.IncreLogEventPipingData;
import org.min.watergap.piping.translator.type.PipingType;

import java.util.LinkedList;
import java.util.List;

/**
 * 关系型数据库执行器
 *
 * @Create by metaX.h on 2022/3/4 22:55
 */
public class MysqlIncreOutFallDrainer extends OutFallDrainer {
    private static final Logger LOG = LogManager.getLogger(MysqlIncreOutFallDrainer.class);

    private List<PipingData> transactionBuffer = new LinkedList<>();

    private boolean isIdentical;

    @Override
    public void init(WaterGapContext waterGapContext) {
        super.init(waterGapContext);
        singleThreadWorkGroups = new SingleThreadWorkGroup[1]; // 单线程消费，方便收集事务信息
        isIdentical = waterGapContext.isIdentical();
    }

    @Override
    protected void doExecute(PipingData dataStruct) {
        // 增量数据处理
        IncreLogEventPipingData increData = (IncreLogEventPipingData)dataStruct;
        if (PipingType.TRANSACTIONBEGIN == increData.getPipingType()) {
            if (!transactionBuffer.isEmpty()) {
                flushTransaction();
            }
        } else if (PipingType.TRANSACTIONEND == increData.getPipingType()) {
            flushTransaction();
        } else if (PipingType.DDLDATA == increData.getPipingType()) {
            // 先清理之前的数据，在执行当前操作
            flushTransaction();
            doDDL(increData);
        } else if (PipingType.ROWDATA == increData.getPipingType()) {
            transactionBuffer.add(increData);
        }
    }

    private void flushTransaction() {
        if(CollectionsUtils.isNotEmpty(transactionBuffer)) {
            IncreLogEventPipingData pipingData = (IncreLogEventPipingData) transactionBuffer.get(0);
            int exeFlag = dataExecutor.executeIncDataWithTrans(pipingData.getHeader().getSchemaName(), transactionBuffer);
            if (exeFlag == 0) {
                throw new WaterGapException(ErrorCode.UPDATE_FAIL, "数据更新异常");
            } else {
                transactionBuffer.clear();
            }
        }
    }

    private void doDDL(IncreLogEventPipingData increData) {
        // 需要根据不同的目标库，转成对应的DDL
        //dataExecutor.execute(increData.getHeader().getSchemaName(), increData.get)
    }

    @Override
    public void destroy() {
        super.destroy();
    }
}
