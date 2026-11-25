# 商品交换约定

`stores` 按 store、operator、from、until 提供经营权，所有适用区间半开且时刻采用带偏移的 ISO 8601。`catalog` 中 local 是门店可供货的身份，pieces 指一销售单位实际包含多少单件。barcode 是门店当时用的识读编码，不独立决定 local。until 为 null 则直到进一步有效修订。

`listings` 中 listing 在 channel 内唯一，push 是某次发布号，local 和展示 pack 标为某一次外部承诺。上报 revision 只在来源与业务键内排序。`amendments` 可同时修订若干关联，changes 中未列出的字段表示此次无意见，clear 指明移除已有字段。显式清除关系会结束该渠道的可用关联，未填不改变已确认关系。同一 channel 加 listing 可以按时间复用，无法决定时运营须暂停重叠窗口。

`channel_receipts` 引用 (channel, listing, push)，不能用来重新认领关联。`purchases` 的 ordered_at 是渠道回传的顾客购买时点，push 指顾客所见，units 表示购买该外部包装的份数。门店须能记录收到的规格不符及经顾客认可的处理。

交换样本为 UTF-8 NDJSON，每行独立。文件中的行顺序保留这组资料的原始提供顺序。
